#!/usr/bin/env python3
"""Drop CodeQL results located in test code from every SARIF file in a folder.

CodeQL's Rust extractor follows the module tree, so `#[cfg(test)] mod tests;`
files are analysed even when .github/codeql/codeql-config.yml lists them in
paths-ignore. Their findings are test vectors (fixed nonces, keys, assert
messages), so .github/workflows/codeql.yml runs this between analyze and
upload. Product code is never filtered. Standard library only.
"""

import json
import pathlib
import re
import sys

TEST_PATH = re.compile(
    r"(^|/)tests(/|\.rs$)"  # tests/ folders and tests.rs modules
    r"|_tests\.rs$"  # foo_tests.rs modules
    r"|(^|/)test/"  # SDK and npm test folders
    r"|^(sim|bench|conformance)/"  # simulations, benchmarks, conformance
    r"|^scripts/test_[^/]*\.py$"  # script tests
)


def in_test_code(result: dict) -> bool:
    locations = result.get("locations") or []
    if not locations:
        return False
    uris = [
        loc.get("physicalLocation", {}).get("artifactLocation", {}).get("uri", "")
        for loc in locations
    ]
    return all(uri and TEST_PATH.search(uri) for uri in uris)


def main(folder: str) -> int:
    files = sorted(pathlib.Path(folder).glob("*.sarif"))
    if not files:
        print(f"no SARIF file in {folder}", file=sys.stderr)
        return 1
    for path in files:
        sarif = json.loads(path.read_text())
        dropped = 0
        for run in sarif.get("runs", []):
            results = run.get("results", [])
            kept = [r for r in results if not in_test_code(r)]
            dropped += len(results) - len(kept)
            run["results"] = kept
        path.write_text(json.dumps(sarif))
        print(f"{path.name}: dropped {dropped} result(s) in test code")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "sarif"))
