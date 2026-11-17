#!/usr/bin/env python3
"""Bounded real-node smoke for the remote room harness; no bucket/load matrix."""
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent


def main():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix='room-bench-smoke-') as scratch:
        out = Path(scratch) / 'smoke.json'
        with open(Path(scratch) / 'node.log', 'w+') as log:
            command = [os.environ.get('WASMKEEP_BIN', str(ROOT / 'target/debug/wasmkeep')),
                       'serve', 'examples/ts-big-room/front.wasm', '--listen', f'127.0.0.1:{port}',
                       '--data-dir', str(Path(scratch) / 'data')]
            for actor in ('room', 'edge', 'relay', 'directory'):
                command += ['--actor', f'{actor}=examples/ts-big-room/{actor}.wasm',
                            '--access', f'{actor}=internal']
            node = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=log)
            deadline = time.monotonic() + 27
            try:
                for _ in range(40):
                    if node.poll() is not None:
                        raise RuntimeError('smoke node exited; check the prebuilt binary/components')
                    try:
                        with urllib.request.urlopen(f'http://127.0.0.1:{port}', timeout=.2):
                            break
                    except OSError:
                        time.sleep(.1)
                else:
                    raise RuntimeError('smoke node did not become ready')
                subprocess.run(['node', 'bench/room-group.mjs', '--url', f'http://127.0.0.1:{port}',
                                '--clients', '24', '--duration', '8', '--rate', '4',
                                '--reconnect-every', '1', '--smoke', '--out', str(out)],
                               cwd=ROOT, check=True, timeout=max(1, deadline - time.monotonic()))
                result = json.loads(out.read_text())
                assert result['incomplete_clients'] == []
                assert len(result['final_cursors']) == 24
                assert len(result['acknowledged_positions']) == result['counters']['publish_ok']
                assert result['ready_at_start'] == 24
                assert result['counters']['deliveries'] >= 24
                assert result['counters']['reconnect_attempts'] > 0
                assert result['counters']['smoke_dropped_frames'] == 1
                assert result['counters']['gaps_recovered'] > 0
                assert result['counters']['recovered_messages'] > 0
                assert result['reconnect_latency']['count'] > 0
                assert result['samples'] and any(s['edges'] is not None for s in result['samples'])
                assert result['delivery_latency']['p99_ms'] is not None
                print('Room benchmark smoke passed: 24 clients, paced publishes, reconnects, HTTP recovery, topology.')
            finally:
                node.terminate()
                try:
                    node.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    node.kill()
                    node.wait()


if __name__ == '__main__':
    main()
