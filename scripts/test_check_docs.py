"""Tests for scripts/check-docs.py: local links and their anchors."""
import pathlib
import shutil
import subprocess
import tempfile
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parent


class Docs(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        (self.root / 'scripts').mkdir()
        for name in ('check-docs.py', 'check-benchmark-paths.py'):
            shutil.copy2(SCRIPTS / name, self.root / 'scripts' / name)
        (self.root / 'docs').mkdir()
        (self.root / 'docs/SUMMARY.md').write_text('- [Guide](guide.md)\n')
        (self.root / 'docs/guide.md').write_text(
            '# The guide\n\n## How it works: `fence` and **records**\n\n## Twice\n\n## Twice\n\n'
            '<a id="custom"></a>\n\n```\n## Not a heading\n```\n'
        )

    def check(self, text):
        (self.root / 'docs/page.md').write_text(text)
        return subprocess.run(
            ['python3', '-I', 'scripts/check-docs.py', 'docs/page.md'],
            cwd=self.root, capture_output=True, text=True,
        )

    def test_good_anchors_pass(self):
        result = self.check(
            '# Page\n\n[a](guide.md#the-guide) [b](guide.md#how-it-works-fence-and-records) '
            '[c](guide.md#twice-1) [d](guide.md#custom) [e](#page) [f](https://x.test/#y)\n'
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_a_missing_anchor_fails(self):
        for link in ('guide.md#nope', '#nope', 'guide.md#not-a-heading', 'guide.md#twice-2'):
            result = self.check(f'# Page\n\n[x]({link})\n')
            self.assertEqual(result.returncode, 1, link)
            self.assertIn('no heading', result.stderr, link)

    def test_a_missing_page_fails(self):
        result = self.check('# Page\n\n[x](gone.md#a)\n')
        self.assertEqual(result.returncode, 1)
        self.assertIn('broken local link', result.stderr)


if __name__ == '__main__':
    unittest.main()
