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
            for actor in ('room', 'pusher', 'edge', 'relay', 'directory'):
                command += ['--actor', f'{actor}=examples/ts-big-room/{actor}.wasm',
                            '--access', f'{actor}=internal']
            # Deployed as the example says (ADR 0040, section 13).
            command += ['--writes', 'pusher=batched']
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
                                '--clients', '16', '--duration', '8', '--rate', '4',
                                '--payload-bytes', '1500', '--stalled-readers', '1',
                                '--stall-after', '1', '--stall-for', '3',
                                '--ramp', 'join:8:2,hold:2,leave:6:2',
                                '--reconnect-every', '1', '--smoke', '--out', str(out)],
                               cwd=ROOT, check=True, timeout=max(1, deadline - time.monotonic()))
                result = json.loads(out.read_text())
                counters = result['counters']
                assert result['incomplete_clients'] == []
                assert result['unacknowledged_publishes'] == []
                assert result['publish_failures_by_status'] == {}
                assert counters['publish_unacknowledged'] == 0
                assert len(result['final_cursors']) == 24
                assert result['joined_total'] == 24 and result['peak_clients'] == 24
                assert counters['clients_left'] == 6 and result['active_at_end'] == 18
                assert len(result['acknowledged_positions']) == counters['publish_ok']
                assert result['ready_at_start'] == 16
                assert counters['deliveries'] >= 24
                # The application is handed each number once, HTTP recovery
                # included; the SDK's discards of what a history page also
                # carried are counted apart (discarded_catchup). A stalled
                # reader catching up over HTTP is bench/test/room-catchup.
                assert counters['repeats'] == 0, counters
                assert counters['discarded_live'] == 0, counters
                assert counters['reconnect_attempts'] > 0
                assert counters['smoke_dropped_frames'] == 1
                assert counters['gaps_recovered'] > 0
                assert counters['recovered_messages'] > 0
                assert result['reconnect_latency']['count'] > 0
                assert result['samples'] and any(s['edges'] is not None for s in result['samples'])
                assert result['delivery_latency']['p99_ms'] is not None
                assert result['payload_bytes'] == 1500 and result['targets'] == 1
                phases = result['phases']
                assert {'0-admit', '1-join', '2-hold', '3-leave', 'drain'} <= set(phases)
                assert phases['1-join']['counters']['clients_joined'] == 8
                assert phases['3-leave']['counters']['clients_left'] == 6
                assert phases['2-hold']['delivery_latency']['count'] > 0
                assert result['topology'] and any(t['phase'] == '3-leave' for t in result['topology'])
                stalled = result['stalled_readers']
                assert stalled['count'] == 1
                assert counters['stall_pauses'] == 1 and counters['stall_resumes'] == 1
                assert stalled['catchup_after_resume']['count'] == 1
                assert stalled['delivery_latency']['count'] > 0
                generator = result['generator']
                assert generator['peak_rss_bytes'] > 0 and generator['peak_cpu_cores'] > 0
                assert 'valid' in generator and generator['peak_loop_delay_p99_ms'] is not None
                print('Room benchmark smoke passed: 24 clients over a short ramp, a stalled reader, 1.5 KB messages, reconnects, HTTP recovery, topology.')
            finally:
                node.terminate()
                try:
                    node.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    node.kill()
                    node.wait()


if __name__ == '__main__':
    main()
