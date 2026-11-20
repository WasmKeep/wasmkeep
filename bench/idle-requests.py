#!/usr/bin/env python3
"""Three local nodes at rest; count every bucket request by node, key and status.

Build wasmkeep and examples/counter first. Start the existing moto test fixture
(e.g. uvx --from 'moto[server]==5.2.5' moto_server -H 127.0.0.1 -p 5055).
Run: python3 bench/idle-requests.py --endpoint http://127.0.0.1:5055 \
  --wasm examples/counter/target/wasm32-wasip2/release/counter.wasm \
  --output bench/results/DATE-COMMIT-idle-bucket-requests.csv
No remote endpoints or credentials are accepted. No actor traffic is generated.
Each GET source must be within 10% of the model (unknown keys fail).
The CSV contains exact key/status counts and separate summed server metrics;
not_found is a subset of GET, never added to it. Startup is excluded.
"""
import argparse
from collections import Counter
import csv
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import threading
import time
from urllib.parse import urlsplit, parse_qs
import urllib.request


def address():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return f'127.0.0.1:{sock.getsockname()[1]}'


def http(url, method='GET', host=None):
    request = urllib.request.Request(url, method=method,
                                     headers={'Host': host} if host else {})
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=5) as r:
        return r.read().decode()


def metrics(addr):
    result = Counter()
    for op, status, value in re.findall(
            r'^wasmkeep_bucket_requests_total\{operation="([^"]+)",status="([^"]+)"\} (\d+)$',
            http(f'http://{addr}/metrics'), re.M):
        result[op, status] += int(value)
    if not result:
        raise RuntimeError('no bucket metrics')
    return result


def check_get_model(counts, apps, elapsed):
    """Fail on an unaccounted GET or a source outside 10% of its cadence."""
    expected = {'nodes': apps * apps, 'control/changed': apps / 3, 'blob-usage': apps / 60}
    expected.update({key: apps / 3 for key in
                     ('current', 'deleted', 'restoring', 'recreated', 'restored', 'secrets/current')})
    measured = Counter()
    for (op, key, status), count in counts.items():
        if op != 'get':
            continue
        if status not in (200, 404):
            raise AssertionError(f'unexpected GET status: {key} {status}')
        if key.startswith('nodes/'):
            source = 'nodes'
        elif key.startswith('state/idle/') and key.split('/')[-1] == 'blob-usage':
            source = 'blob-usage'
        elif key.startswith('apps/idle/'):
            source = '/'.join(key.split('/')[3:])
        else:
            source = key
        if source not in expected:
            raise AssertionError(f'unmodeled GET: {key}')
        measured[source] += count / elapsed
    for source, rate in expected.items():
        if abs(measured[source] / rate - 1) > .10:
            raise AssertionError(f'{source}: measured {measured[source]:.6f}, model {rate:.6f}')
    return measured


class Proxy(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.counts = Counter()
        self.lock = threading.Lock()
        super().__init__(('127.0.0.1', 0), Handler)

    def snapshot(self):
        with self.lock:
            return self.counts.copy()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def forward(self):
        body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        if self.headers.get('Transfer-Encoding') == 'chunked':
            chunks = []
            while size := int(self.rfile.readline().split(b';')[0], 16):
                chunks.append(self.rfile.read(size))
                self.rfile.read(2)
            while self.rfile.readline() != b'\r\n':
                pass
            body = b''.join(chunks)
        headers = {k: v for k, v in self.headers.items()
                   if k.lower() not in ('transfer-encoding', 'connection', 'content-length')}
        headers['Content-Length'] = str(len(body))
        conn = HTTPConnection(self.server.endpoint.hostname,
                                          self.server.endpoint.port, timeout=10)
        try:
            conn.request(self.command, self.path, body, headers)
            response = conn.getresponse()
            data = response.read()
            path = urlsplit(self.path)
            query = parse_qs(path.query)
            op = 'list' if self.command == 'GET' and 'list-type' in query else self.command.lower()
            key = query.get('prefix', [path.path.split('/', 2)[-1]])[0] if op == 'list' else path.path.split('/', 2)[-1]
            with self.server.lock:
                self.server.counts[op, key, response.status] += 1
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in ('transfer-encoding', 'connection', 'content-length', 'server', 'date'):
                    self.send_header(key, value)
            length = response.getheader('Content-Length', '0') if self.command == 'HEAD' else str(len(data))
            self.send_header('Content-Length', length)
            self.end_headers()
            if self.command != 'HEAD':
                self.wfile.write(data)
        finally:
            conn.close()

    do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = forward


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--binary', default='target/debug/wasmkeep')
    p.add_argument('--wasm', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--seconds', type=int, default=120)
    p.add_argument('--warmup', type=int, default=40)
    args = p.parse_args()
    endpoint = urlsplit(args.endpoint)
    if endpoint.scheme != 'http' or endpoint.hostname != '127.0.0.1' or endpoint.path not in ('', '/'):
        p.error('endpoint must be an HTTP loopback fixture')
    if args.seconds < 60 or args.seconds % 60 or args.warmup < 30 or args.seconds + args.warmup >= 3500:
        p.error('use whole-minute windows and at least 30 seconds warmup; total must be below 3500 seconds (hourly maintenance excluded)')
    binary, wasm = str(Path(args.binary).resolve()), str(Path(args.wasm).resolve())
    env = {**os.environ, 'AWS_ACCESS_KEY_ID': 'test', 'AWS_SECRET_ACCESS_KEY': 'test',
           'AWS_REGION': 'us-east-1', 'AWS_ALLOW_HTTP': 'true', 'AWS_ENDPOINT': args.endpoint,
           'TOKIO_WORKER_THREADS': '2'}
    env.pop('AWS_SESSION_TOKEN', None)
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    with open(args.output, 'w', newline='') as output:
        writer = csv.writer(output)
        writer.writerow(['commit', 'apps', 'node', 'seconds', 'source', 'operation', 'key', 'status', 'count', 'per_s'])
        for apps in (1, 2):
            with tempfile.TemporaryDirectory(prefix='idle-requests-') as temp:
                root = Path(temp)
                bucket = root.name
                http(f'{args.endpoint}/{bucket}', 'PUT')
                children, proxies, logs, addrs = [], [], [], []
                try:
                    for app in range(apps):
                        project = root / f'app{app}.toml'
                        project.write_text(f'[app]\ntenant="idle"\nname="app{app}"\n[classes.counter]\ncomponent={json.dumps(wasm)}\naccess="open"\n')
                        subprocess.run([binary, 'deploy', '--project', str(project), '--bucket', f's3://{bucket}'],
                                       env=env, check=True, stdout=subprocess.DEVNULL)
                    for node in 'abc':
                        proxy = Proxy(endpoint)
                        proxies.append(proxy)
                        threading.Thread(target=proxy.serve_forever, daemon=True).start()
                        public, cluster, metric = address(), address(), address()
                        addrs.append((public, metric))
                        config = root / f'{node}.toml'
                        config.write_text(f'[process]\nid="{node}"\ndata-dir={json.dumps(str(root / node))}\n'
                            f'[listen]\npublic="{public}"\npublic-advertise="http://{public}"\n'
                            f'cluster="{cluster}"\ncluster-advertise="https://{cluster}"\nmetrics="{metric}"\n' +
                            ''.join(f'[[app]]\nname="idle/app{i}"\nnode-id="{node}-{i}"\nhostnames=["app{i}.test"]\n' for i in range(apps)))
                        log = (root / f'{node}.log').open('w+')
                        logs.append(log)
                        children.append(subprocess.Popen([binary, 'serve', '--node-file', str(config),
                            '--bucket', f's3://{bucket}'], env={**env, 'AWS_ENDPOINT': f'http://127.0.0.1:{proxy.server_port}'},
                            stdout=log, stderr=log))
                    deadline = time.monotonic() + 60
                    for public, _ in addrs:
                        for i in range(apps):
                            while True:
                                if any(c.poll() is not None for c in children):
                                    raise RuntimeError('node exited')
                                try:
                                    http(f'http://{public}/_wasmkeep/ready', host=f'app{i}.test')
                                    break
                                except OSError:
                                    if time.monotonic() > deadline:
                                        raise RuntimeError('readiness timeout')
                                    time.sleep(.2)
                    print(f'{apps} app(s): warming for {args.warmup}s', flush=True)
                    time.sleep(args.warmup)
                    before_metrics = [metrics(m) for _, m in addrs]
                    before = [proxy.snapshot() for proxy in proxies]
                    start = time.monotonic()
                    print(f'{apps} app(s): measuring {args.seconds}s', flush=True)
                    time.sleep(args.seconds)
                    after = [proxy.snapshot() for proxy in proxies]
                    elapsed = time.monotonic() - start
                    after_metrics = [metrics(m) for _, m in addrs]
                    if any(c.poll() is not None for c in children):
                        raise RuntimeError('node exited during window')
                    for n, node in enumerate('abc'):
                        for (op, key, status), count in sorted((after[n] - before[n]).items()):
                            writer.writerow([commit, apps, node, f'{elapsed:.6f}', 'wire', op, key, status, count, f'{count / elapsed:.6f}'])
                        for (op, status), count in sorted((after_metrics[n] - before_metrics[n]).items()):
                            writer.writerow([commit, apps, node, f'{elapsed:.6f}', 'metric', op, '', status, count, f'{count / elapsed:.6f}'])
                        get_count = sum(c for (op, _, _), c in (after[n] - before[n]).items() if op == 'get')
                        metric_get = sum(c for (op, _), c in (after_metrics[n] - before_metrics[n]).items() if op == 'get')
                        print(f'  {node}: {get_count / elapsed:.4f} GET/s; wire={get_count}, metrics={metric_get}', flush=True)
                        if abs(get_count - metric_get) > 10:
                            raise RuntimeError('wire/metric mismatch exceeds snapshot boundary allowance')
                        for label, code in [('ok', 200), ('not_found', 404)]:
                            wire = sum(c for (op, _, status), c in (after[n] - before[n]).items()
                                       if op == 'get' and status == code)
                            metric = (after_metrics[n] - before_metrics[n])['get', label]
                            if abs(wire - metric) > 10:
                                raise RuntimeError(f'GET status mismatch: {label}: {wire} vs {metric}')
                        check_get_model(after[n] - before[n], apps, elapsed)
                    output.flush()
                except BaseException:
                    for log in logs:
                        log.flush()
                        log.seek(0)
                        print(log.read()[-6000:])
                    raise
                finally:
                    for child in children:
                        child.terminate()
                    for child in children:
                        try:
                            child.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            child.kill()
                            child.wait()
                    for proxy in proxies:
                        proxy.shutdown()
                        proxy.server_close()
                    for log in logs:
                        log.close()


if __name__ == '__main__':
    main()
