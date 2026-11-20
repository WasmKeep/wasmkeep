"""The idle bench must account for all GETs, including negative probes."""
from collections import Counter
import importlib.util
import io
from types import SimpleNamespace
import threading
from unittest.mock import Mock, patch
from urllib.parse import urlsplit
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    'idle_requests', Path(__file__).resolve().parents[1] / 'bench/idle-requests.py')
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class IdleAccounting(unittest.TestCase):
    def test_proxy_counts_success_missing_and_list_separately(self):
        server = SimpleNamespace(endpoint=urlsplit('http://127.0.0.1:5055'),
                                 counts=Counter(), lock=threading.Lock())
        for path, status in [('/bucket/nodes/a', 200), ('/bucket/apps/idle/app0/deleted', 404),
                             ('/bucket?list-type=2&prefix=nodes%2F', 200)]:
            handler = bench.Handler.__new__(bench.Handler)
            handler.server, handler.command, handler.path = server, 'GET', path
            handler.headers = {}
            handler.rfile, handler.wfile = io.BytesIO(), io.BytesIO()
            handler.send_response = Mock()
            handler.send_header = Mock()
            handler.end_headers = Mock()
            response = Mock(status=status)
            response.read.return_value = b'body'
            response.getheaders.return_value = []
            with patch.object(bench, 'HTTPConnection') as connection:
                connection.return_value.getresponse.return_value = response
                handler.forward()
                connection.return_value.close.assert_called_once()
            handler.send_response.assert_called_once_with(status)
            self.assertEqual(handler.wfile.getvalue(), b'body')
        self.assertEqual(server.counts, {
            ('get', 'nodes/a', 200): 1,
            ('get', 'apps/idle/app0/deleted', 404): 1,
            ('list', 'nodes/', 200): 1,
        })

    def fixture(self, apps):
        counts = Counter()
        for node in 'abc':
            for app in range(apps):
                counts['get', f'nodes/{node}-{app}', 200] = 40 * apps
        counts['get', 'control/changed', 404] = 40 * apps
        for app in range(apps):
            counts['get', f'state/idle/app{app}/blob-usage', 404] = 2
            for key in ('current', 'deleted', 'restoring', 'recreated', 'restored', 'secrets/current'):
                counts['get', f'apps/idle/app{app}/{key}', 200 if key == 'current' else 404] = 40
        return counts

    def test_one_and_two_apps_include_negative_gets(self):
        for apps, total, missing in [(1, 402, 242), (2, 1044, 484)]:
            counts = self.fixture(apps)
            rates = bench.check_get_model(counts, apps, 120)
            self.assertAlmostEqual(sum(rates.values()), total / 120)
            self.assertEqual(sum(c for (_, _, status), c in counts.items() if status == 404), missing)

    def test_omitted_safety_reads_fail(self):
        for key in ('restoring', 'recreated', 'restored'):
            counts = self.fixture(1)
            del counts['get', f'apps/idle/app0/{key}', 404]
            with self.assertRaisesRegex(AssertionError, key):
                bench.check_get_model(counts, 1, 120)

    def test_extra_requests_and_unknown_keys_fail(self):
        counts = self.fixture(1)
        counts['get', 'apps/idle/app0/current', 200] *= 2
        with self.assertRaisesRegex(AssertionError, 'current'):
            bench.check_get_model(counts, 1, 120)
        counts = self.fixture(1)
        counts['get', 'unexpected', 200] = 1
        with self.assertRaisesRegex(AssertionError, 'unmodeled GET'):
            bench.check_get_model(counts, 1, 120)


if __name__ == '__main__':
    unittest.main()
