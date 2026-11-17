"""Restore the known soak fixture only; never start arbitrary archived code."""
import argparse
import csv
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid


def run(args):
    try:
        return subprocess.run(args, check=True, capture_output=True, timeout=300).stdout
    except subprocess.CalledProcessError as error:
        # Preserve the underlying CLI error, even after scratch cleanup.
        print(error.stderr.decode(errors="replace"), file=sys.stderr, end="", flush=True)
        raise


def bucket_url(value):
    match = re.fullmatch(r"s3://([a-zA-Z0-9.-]+)(?:/([a-zA-Z0-9/_-]+))?", value)
    if not match:
        raise ValueError("expected an S3 bucket URL with a simple prefix")
    return match[1], (match[2] or "").strip("/")


def disjoint(source, scratch):
    sb, sp = bucket_url(source)
    db, dp = bucket_url(scratch)
    if sb == db and (not sp or not dp or sp == dp or
                     sp.startswith(dp + "/") or dp.startswith(sp + "/")):
        raise ValueError("source and scratch roots must not overlap")


def floors(path):
    data = json.loads(Path(path).read_text())
    if not isinstance(data, dict) or not data:
        raise ValueError("acknowledgement snapshot must be a nonempty path-to-count object")
    for route, value in data.items():
        if not re.fullmatch(r"/actor/t/[a-zA-Z0-9_-]+/durable", route):
            raise ValueError("only soak fixture durable routes are accepted")
        if type(value) is not int or value <= 0:
            raise ValueError("each sampled actor must have acknowledged durable writes")
    return data


def address():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{listener.getsockname()[1]}"


def http(addr, path, method="GET"):
    request = urllib.request.Request(f"http://{addr}{path}", method=method,
                                     data=b"" if method == "POST" else None)
    # Ignore proxy environment variables for private listeners.
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=5) as r:
        return r.read().decode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", help="running soak fixture's S3 root; never mutated by this drill")
    parser.add_argument("--acks", help="JSON route -> max durable count, captured before invocation")
    parser.add_argument("--output", help="CSV measurement file")
    args = parser.parse_args()
    if bool(args.source) != bool(args.acks):
        parser.error("--source and --acks must be supplied together")
    expected = floors(args.acks) if args.acks else {}
    bucket = os.environ["WASMKEEP_TEST_S3_BUCKET"]
    if not re.fullmatch(r"[a-zA-Z0-9.-]+", bucket):
        raise ValueError("WASMKEEP_TEST_S3_BUCKET must be a bucket name, not a prefix")
    scratch = f"s3://{bucket}/restore-drill-{uuid.uuid4().hex}"
    source = args.source or scratch + "/source"
    if args.source:
        disjoint(source, scratch)
    archive, restored = scratch + "/archive", scratch + "/restored"
    binary = str(Path(os.environ["DRILL_BIN"]).resolve())
    wasm = Path(os.environ["DRILL_WASM"]).resolve()
    fixture = wasm.read_bytes()
    aws = ["aws", "--no-cli-pager"]
    if os.environ.get("AWS_ENDPOINT"):
        aws += ["--endpoint-url", os.environ["AWS_ENDPOINT"]]
    aws += ["s3api"]
    # aws CLI is used only for scratch enumeration, fixture checks and cleanup.
    def objects(url):
        b, p = bucket_url(url)
        return json.loads(run(aws + ["list-objects-v2", "--bucket", b,
                                    "--prefix", p + "/", "--output", "json"])).get("Contents", [])

    def cli(*words):
        return run([binary, *words])

    children, logs = [], []
    stop = threading.Event()
    writer = None
    rows = []
    failure = None
    owned = False
    def measure(name, action):
        start = time.monotonic_ns()
        result = action()
        rows.append((name, time.monotonic_ns() - start, "ns"))
        return result

    with tempfile.TemporaryDirectory(prefix="wasmkeep-drill-") as directory:
        directory = Path(directory)
        # An exact project replaces archived configuration, fronts and policies.
        # Only the owned scratch root is redeployed; an external source is read-only.
        project = directory / "wasmkeep.toml"
        project.write_text(
            '[app]\ntenant = "restore-drill"\nname = "fixture"\n'
            '[classes.t]\ncomponent = ' + json.dumps(str(wasm)) + '\naccess = "open"\n')
        def node(root, name):
            addr, metrics = address(), address()
            log = (directory / f"{name}.log").open("wb")
            logs.append(log)
            # Follow restore fences through the rooted, audited fixture deploy.
            # The restored deploy is replaced with the clean local project below.
            child = subprocess.Popen([binary, "serve", "--no-secrets", "--app", "restore-drill/fixture",
                "--bucket", root, "--node-id", name, "--listen", addr,
                "--advertise", f"http://{addr}", "--cluster-plaintext", "accept",
                "--metrics-listen", metrics, "--data-dir", str(directory / name)],
                stdout=log, stderr=log, env={**os.environ, "TOKIO_WORKER_THREADS": "2"})
            children.append(child)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if child.poll() is not None:
                    raise RuntimeError(f"{name} exited before readiness")
                try:
                    http(addr, "/_wasmkeep/ready")
                    return addr, metrics
                except (OSError, urllib.error.URLError):
                    time.sleep(0.2)
            raise RuntimeError(f"{name} did not become ready")

        try:
            if objects(scratch):
                raise RuntimeError("scratch root is not empty")
            owned = True
            if not args.source:
                cli("deploy", "--app", "restore-drill/fixture", "--no-project", "--bucket", source,
                    "--actor", f"t={wasm}")
                first, _ = node(source, "source-a")
                second, _ = node(source, "source-b")
                routes = [f"/actor/t/drill-{i}/durable" for i in range(4)]
                for route in routes:
                    expected[route] = int(http(first, route, "POST"))
                # Modest real traffic throughout create/verify/restore, not a load test.
                errors, acknowledgements = [], []
                def write():
                    turn = 0
                    while not stop.wait(0.1):
                        try:
                            route = routes[turn % len(routes)]
                            value = int(http([first, second][turn % 2], route, "POST"))
                            acknowledgements.append((route, value))
                            turn += 1
                        except Exception as error:
                            errors.append(str(error))
                            return
                writer = threading.Thread(target=write)
                writer.start()
            before_create = len(acknowledgements) if writer else 0
            measure("backup_create", lambda: cli("backup", "create", "--bucket", source,
                    "--app", "restore-drill/fixture", "--to", archive))
            if writer:
                during_create = len(acknowledgements) - before_create
                if not during_create:
                    raise RuntimeError("no durable write acknowledged during backup create")
                rows.append(("writes_acked_during_create", during_create, "count"))
            measure("backup_verify", lambda: cli("backup", "verify", "--from", archive))
            if objects(restored):
                raise RuntimeError("restore root is not empty")
            measure("backup_restore", lambda: cli("backup", "restore", "--from", archive,
                    "--app", "restore-drill/fixture", "--bucket", restored))
            # Fail closed for a non-fixture deploy, before starting any restored actor.
            components = [o for o in objects(restored) if o["Key"].endswith(".wasm")]
            if not components:
                raise RuntimeError("restore contains no deployed fixture")
            for obj in components:
                if not obj["Key"].endswith("/t.wasm"):
                    raise RuntimeError("drill accepts only the soak's t fixture, no front")
                dest = directory / "component.wasm"
                run(aws + ["get-object", "--bucket", bucket, "--key", obj["Key"], str(dest)])
                if dest.read_bytes() != fixture:
                    raise RuntimeError("archived component differs from the local soak fixture")
            cli("deploy", "--project", str(project), "--no-build", "--bucket", restored)
            addr, metrics = measure("restored_start", lambda: node(restored, "restored"))
            for route, floor in expected.items():
                actual = int(http(addr, route))
                if actual < floor:
                    raise RuntimeError(f"{route}: restored {actual}, acknowledged {floor}")
            rows.append(("actors_checked", len(expected), "count"))
            rows.append(("acknowledged_floor", sum(expected.values()), "count"))
            # These counters belong to this new node alone, not the backup CLI.
            for line in http(metrics, "/metrics").splitlines():
                match = re.fullmatch(r'wasmkeep_bucket_requests_total\{operation="([^"]+)",status="([^"]+)"\} (\d+)', line)
                if match:
                    rows.append((f"restored_node/{match[1]}/{match[2]}", int(match[3]), "count"))
            if writer:
                stop.set()
                writer.join()
                if errors or not acknowledgements:
                    raise RuntimeError(f"online writer failed or made no progress: {errors}")
                rows.append(("concurrent_writes_acked", len(acknowledgements), "count"))
        except BaseException as error:
            failure = error
        finally:
            stop.set()
            if writer:
                writer.join(timeout=10)
            for child in reversed(children):
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            for log in logs:
                log.close()
            if failure:
                for log in directory.glob("*.log"):
                    print(log.read_text(errors="replace")[-4000:])
            # Exact generated prefix, with a trailing slash: never source or bucket root.
            cleanup = ["aws", "--no-cli-pager"]
            if os.environ.get("AWS_ENDPOINT"):
                cleanup += ["--endpoint-url", os.environ["AWS_ENDPOINT"]]
            if owned:
                run(cleanup + ["s3", "rm", scratch + "/", "--recursive", "--only-show-errors"])
            if owned and objects(scratch):
                raise RuntimeError("scratch objects remain after cleanup")
    if failure:
        raise failure
    output = Path(args.output or f"bench/results/{time.strftime('%Y-%m-%d')}-restore-drill-{uuid.uuid4().hex[:8]}.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    commit = run(["git", "rev-parse", "HEAD"]).decode().strip()
    if subprocess.run(["git", "diff", "--quiet", "HEAD"], check=False).returncode:
        commit += "-dirty"
    with output.open("x", newline="") as handle:
        out = csv.writer(handle)
        out.writerow(["commit", "scope", "metric", "value", "unit"])
        out.writerows((commit, "soak" if args.source else "fixture", *row) for row in rows)
    print(f"restore drill passed; measurements: {output}")


if __name__ == "__main__":
    def interrupted(signum, _frame):
        raise RuntimeError(f"drill interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    main()
