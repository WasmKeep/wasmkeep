"""Reject local paths in staged documentation/benchmarks, a tree, or a PR range.

This is a path-publication check, not a general secret scanner. Read Git blobs
so unstaged redactions cannot hide private text that will be committed. Range
mode checks changed blobs in every introduced commit, including intermediate
content removed before HEAD, without rescanning untouched inherited files.
Diagnostics omit matched text. Only Python's standard library is used.
"""

import argparse
import ipaddress
import json
import os
import re
import subprocess
import sys
import urllib.parse


SCOPES = ("bench", "docs")
SYSTEM_ROOTS = ("/proc", "/sys", "/usr", "/bin", "/sbin", "/etc", "/lib", "/lib64")
SYSTEM_DEVICES = {"/dev/null", "/dev/zero", "/dev/random", "/dev/urandom", "/dev/stdin", "/dev/stdout", "/dev/stderr"}
BARE_MARKERS = {"~/", "/tmp", "/tmp/"}
# A URL ends where a path list or a query begins: what follows a delimiter,
# or the query and fragment, is scanned like any other text.
URL = re.compile(r"(?:https?|s3)://[^\s<>\"'`|,;()?#]+", re.IGNORECASE)
FILE_URI = re.compile(r"\bfile:(?://[^/\s<>\"'`]*)?", re.IGNORECASE)
UNICODE_ESCAPE = re.compile(r"\\u([0-9a-f]{4})", re.IGNORECASE)
ESCAPED_WHITESPACE = re.compile(r"(?<!\\)((?:\\\\)*)\\[nrt]")
# Markdown delimiters at a token boundary are not part of a relative filename.
# Keep glob/filename prefixes such as crates/*/tests and build_/logs intact.
EMPHASIS = re.compile(r"(?<![\w./\\])(?:\*+|_+)(?=[/~])")
IPV6_CIDR = re.compile(r"(?<![\w:.])[0-9a-f:.]*:[0-9a-f:.]+/[0-9]{1,3}(?!\w)", re.IGNORECASE)
# Match the entire quoted span: punctuation, whitespace and apostrophes can
# belong to a UNC share name, so masking its prefix could conceal a path.
REGEX_NOTATION = re.compile(r"""(?P<quote>[\x60"'])(?:\\\\[sSdDwWbBAZz](?:[+*?]|\{[0-9]+(?:,[0-9]*)?\})?){2,}(?P=quote)""")
PRINTF_FORMAT = re.compile(r"(?P<prefix>\bprintf[ \t]+(?P<quote>[\"']))(?P<body>[^\"'\r\n]*)(?P=quote)(?=$|\s)")
# The reviewed example's literal suffix is not a general UNC-share exception.
FORMAT_NOTATION = re.compile(r"^(?:\\\\[nrt]){2,}(?:indent)?$")
DRIVE_PREFIX = re.compile(r"(?<!\w)[A-Za-z]:[\\/]")
# `<word ...>` placeholders become `{word_...}` and HTML closing tags go;
# a shell redirection (`>/home/x`) has no opening `<word` and stays checked.
PLACEHOLDER = re.compile(r"<([A-Za-z][\w .,-]{0,40})>")
HTML_CLOSE = re.compile(r"</[A-Za-z][\w-]*>")
# Placeholders that stand for a directory: `{word}`, `${VAR}`, `[repo]` and
# `[scratch]` become plain words, so what follows is a relative path. Any
# other bracket before a slash (a log tag such as `[INFO]`) hides nothing.
BRACED = re.compile(r"(?:\$?\{([\w .,-]{1,40})\}|\[(repo|scratch)\])(?=/)")
# A one-letter option glued to a path (`-I/x`, `-L/x`) is not a word.
OPTION_PREFIX = re.compile(r"(?<![\w-])-[A-Za-z](?=/)")
ANSI_CSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
# A home directory anywhere in a path, absolute or after a placeholder.
HOME_DIR = re.compile(r"/(?:home|Users)/[\w.-]+")
PROC_ROOT = re.compile(r"^/proc/[^/]+/(?:root|cwd)(?:/|$)")
UNC_PREFIX = re.compile(r"\\\\[?.]\\")
PATH = re.compile(
    r"(?<![\w])(?P<drive>[A-Za-z]:[\\/][^\s`\"'<>\[\]()|,:;]*)"
    r"|(?<![\w])(?P<home>~[\w.-]*/[^\s`\"'<>\[\]()|,:;]*)"
    r"|(?<![\w./\\~*])(?P<posix>(?>/+)(?![\s*!])[^\s`\"'<>\[\]()|,:;]+)"
    r"|(?<![\w\\])(?P<unc>\\{2,}[A-Za-z0-9][A-Za-z0-9._-]*\\+[^\\\s`\"'<>\[\]()|,:;]+[^\s`\"'<>\[\]()|,:;]*)"
)

# These are WasmKeep HTTP namespaces wherever they appear in documentation,
# rather than private directories. Other roots still require a reviewed example.
PUBLIC_DOC_ROUTE_PREFIXES = ("/actor/", "/_wasmkeep/")
PUBLIC_DOC_ROUTES = {"/metrics"}

# Additional application routes and deployment examples, reviewed per file. Match
# complete candidates rather than exempting a whole line, directory, or /var.
REVIEWED_PATHS = {
    "bench/compare/src/main.rs": {"/actor/counter/{name}", "/counter/{name}", "/work"},
    "bench/compare/src/cluster.rs": {
        "/actor/counter/{name}", "/counter/{name}",
        "/actor/ledger/{name}", "/ledger/{name}",
    },
    "bench/celld/cluster.js": {"/ledger/", "/ledger/{name}"},
    "bench/workerd/config.capnp": {"/workerd/workerd.capnp"},
    "bench/workerd/counter.js": {"/counter/", "/counter/{name}"},
    "docs/benchmarks.md": {"/work"},
    "docs/concepts.md": {"/history?limit=20"},
    "docs/dogfood.md": {"/monitors", "/config"},
    "docs/deploy/three-nodes.md": {"/var/lib/wasmkeep/data", "/var/lib/wasmkeep/cache"},
    "docs/deploy/front-door.md": {"/actor"},
    "docs/deploy/single-node.md": {
        "/var/lib/wasmkeep",
        "/var/lib/wasmkeep/classes/", "/var/lib/wasmkeep/classes/chat_room.wasm",
        "/var/lib/wasmkeep/classes/counter.wasm", "/var/lib/wasmkeep/data", "/var/lib/wasmkeep/cache",
        "/backup/$db",
    },
    "docs/tutorials/first-actor.md": {"/?who=alice"},
}
# A checkout may add reviewed paths for files of its own that this list does
# not know (notes kept outside the published tree, for example) in
# scripts/reviewed-paths.local.json: {"paths": {"file": ["/route", ...]},
# "doc_route_prefixes": ["/prefix/", ...]}. Absent, nothing is added.
_LOCAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reviewed-paths.local.json")
if os.path.exists(_LOCAL):
    with open(_LOCAL, encoding="utf-8") as _f:
        _extra = json.load(_f)
    for _path, _routes in _extra.get("paths", {}).items():
        REVIEWED_PATHS.setdefault(_path, set()).update(_routes)
    PUBLIC_DOC_ROUTE_PREFIXES += tuple(_extra.get("doc_route_prefixes", ()))

# This shell expression is a documented backup destination, not a workstation
# path. Mask only this exact expression; other paths on the line remain checked.
REVIEWED_LITERALS = {
    "docs/deploy/single-node.md": (
        '/backup/$(dirname "$db")', "/var/lib/wasmkeep/{classes,data,cache}",
    ),
    # Reviewed sed programs, a find glob, suffix removal and a dirname-based
    # relative path contain slash-delimited syntax, not absolute filenames.
    "bench/run.sh": (
        "'s/^Model name: *//p'",
        r"'s/.*failover_ms=\([0-9]*\).*/\1/p'",
        r'"s/,\"$commit\",/,\"$commit-dirty\",/"',
        "'*/new/estimates.json'", "${bench%/new/estimates.json}",
        '$(dirname "$0")/..',
    ),
    # The same reviewed expressions in the soak's runner, and its sed
    # program reading the soak's metric lines.
    # The same reviewed expressions in the three-node comparison's runner.
    "bench/cluster.sh": (
        "'s/^Model name: *//p'",
        '$(dirname "$0")/..',
    ),
    # The same reviewed expressions in the one-node bucket comparison's runner.
    "bench/bucket.sh": (
        "'s/^Model name: *//p'",
        '$(dirname "$0")/..',
    ),
    "bench/soak.sh": (
        "'s/^Model name: *//p'",
        r"'s/^soak_metric \([^ ]*\) \([0-9]*\) \([a-z]*\)$/\1 \2 \3/p'",
        '$(dirname "$0")/..',
    ),
}


def git(*args):
    return subprocess.check_output(["git", *args], stderr=subprocess.PIPE)


def resolve(revision, kind):
    # Resolve first: option-like user arguments must not become Git flags.
    return git("rev-parse", "--verify", "--end-of-options", revision + "^{" + kind + "}").decode().strip()


def revisions(args, parser):
    if args.range is None:
        return [resolve(args.rev, "tree") if args.rev else None]
    if "..." in args.range or args.range.count("..") != 1:
        parser.error("--range requires BASE..HEAD with both endpoints")
    base, head = args.range.split("..")
    if not base or not head:
        parser.error("--range requires BASE..HEAD with both endpoints")
    base, head = resolve(base, "commit"), resolve(head, "commit")
    return git("rev-list", "--reverse", base + ".." + head).decode().splitlines()


def changed_blobs(revision):
    """Read this commit's changes, including changes created by a merge.

    Each introduced side-branch commit is checked separately by rev-list. A
    merge is compared with its first parent so its own resolutions/imports are
    also checked. Root commits compare against the empty tree. Disable rename
    detection so an existing blob published at a new path is checked there.
    """
    parents = git("rev-list", "--parents", "-n", "1", revision).decode().split()[1:]
    endpoints = (parents[0], revision) if parents else (revision,)
    listing = git(
        "diff-tree", "--root", "--no-commit-id", "--raw", "-r", "-z", "--no-abbrev",
        "--no-relative", "--no-renames", "--diff-filter=AMT", *endpoints, "--",
        *(f":(top){scope}" for scope in SCOPES),
    ).split(b"\0")
    for metadata, path in zip(listing[::2], listing[1::2]):
        fields = metadata.split()
        if fields[1] == b"160000":
            continue
        yield path.decode(errors="replace"), fields[3].decode()


def blobs(revision, changed_only=False):
    if revision and changed_only:
        yield from changed_blobs(revision)
        return
    if revision:
        listing = git("ls-tree", "-rz", "--full-tree", revision, "--", *SCOPES)
    else:
        listing = git(
            "ls-files", "--full-name", "--stage", "-z", "--",
            *(f":(top){scope}" for scope in SCOPES),
        )
    for entry in listing.split(b"\0"):
        if not entry:
            continue
        metadata, path_bytes = entry.split(b"\t", 1)
        fields = metadata.split()
        # A gitlink names a commit, not a blob (both in the index and a tree).
        if fields[0] == b"160000" or (revision and fields[1] != b"blob"):
            continue
        path = path_bytes.decode(errors="replace")
        oid = fields[2] if revision else fields[1]
        yield path, oid.decode()


def normalize(line):
    # Decode common JSON and URI representations, including nested encoding.
    for _ in range(4):
        decoded = UNICODE_ESCAPE.sub(lambda match: chr(int(match[1], 16)), line)
        decoded = urllib.parse.unquote(decoded).replace(r"\/", "/")
        if decoded == line:
            break
        line = decoded
    return ANSI_CSI.sub("", line)


def mask_ipv6_cidr(match):
    try:
        ipaddress.IPv6Network(match[0], strict=False)
    except ValueError:
        return match[0]
    return " "


def problem_line(path, line, root):
    if UNC_PREFIX.search(line):
        return "Windows absolute path"
    line = HTML_CLOSE.sub(" ", line)
    line = PLACEHOLDER.sub(lambda match: "{" + match[1] + "}", line)
    # A file: URI still names a local path, unlike a public HTTP or S3 URL.
    line = FILE_URI.sub(" ", line)
    # A URL path is not an absolute local filename. Do not exempt its whole line.
    line = URL.sub(" ", line)
    # A drive prefix is decisive even when following components look like
    # regex/printf escapes (for example an encoded Windows path).
    if DRIVE_PREFIX.search(line):
        return "Windows absolute path"
    for literal in REVIEWED_LITERALS.get(path, ()):
        line = line.replace(literal, " ")
    line = IPV6_CIDR.sub(mask_ipv6_cidr, line)
    line = REGEX_NOTATION.sub(" ", line)
    # Formatting escapes are ambiguous with one-letter UNC hosts; exempt them
    # only in printf's quoted format, leaving any following path to be scanned.
    line = PRINTF_FORMAT.sub(
        lambda match: match["prefix"] + FORMAT_NOTATION.sub(" ", match["body"]) + match["quote"],
        line,
    )
    line = EMPHASIS.sub(" ", line)
    line = OPTION_PREFIX.sub("- ", line)
    line = BRACED.sub(lambda match: (match[1] or match[2]).replace(" ", "_"), line)
    if HOME_DIR.search(line):
        return "home directory"
    for match in PATH.finditer(line):
        candidate = match[0].rstrip(",:;")
        # Trim sentence punctuation without erasing a terminal dot component.
        if candidate.rsplit("/", 1)[-1] not in {".", ".."}:
            candidate = candidate.rstrip(".")
        if candidate.strip("/") == "" or candidate in BARE_MARKERS:
            continue
        if match.lastgroup == "posix":
            # Extra leading slashes name the same file.
            candidate = "/" + candidate.lstrip("/")
        if match.lastgroup != "posix":
            return "home-relative directory" if match.lastgroup == "home" else "Windows absolute path"
        # Do not let a permitted lexical prefix conceal a traversal elsewhere.
        # Rejecting parent components is conservative even when they end inside
        # another system directory; published examples can use the direct path.
        if ".." in candidate.split("/"):
            return "absolute path with parent-directory traversal"
        # Even under a standard prefix, the current checkout itself is private.
        if candidate == root or candidate.startswith(root + "/"):
            return "absolute checkout directory"
        if PROC_ROOT.match(candidate):
            return "absolute path through another process's root"
        if candidate in SYSTEM_DEVICES or any(candidate == prefix or candidate.startswith(prefix + "/") for prefix in SYSTEM_ROOTS):
            continue
        if candidate in REVIEWED_PATHS.get(path, ()):
            continue
        if path.startswith("docs/"):
            if candidate in PUBLIC_DOC_ROUTES or candidate.startswith(PUBLIC_DOC_ROUTE_PREFIXES):
                continue
            # Only the documentation namespace is a generic site-relative link.
            # Markdown syntax alone must not hide ](/home/...) or ](/mnt/...).
            if candidate.startswith("/docs/") and line[max(0, match.start() - 2):match.start()] == "](":
                continue
        return "absolute local path"
    return None


def problem(path, line, root):
    line = normalize(line)
    # Inspect the original too: plain Windows paths can contain backslash-t/r/n.
    category = problem_line(path, line, root)
    if category:
        return category
    # An odd backslash run escapes whitespace; paired backslashes stay literal.
    separated = ESCAPED_WHITESPACE.sub(lambda match: match[1] + " ", line)
    if separated != line:
        return problem_line(path, separated, root)
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--rev", help="inspect this committed tree instead of the index")
    mode.add_argument("--range", help="inspect every commit introduced by BASE..HEAD")
    args = parser.parse_args()
    trees = revisions(args, parser)
    problems = 0
    seen = set()
    root = git("rev-parse", "--show-toplevel").decode().strip()
    for revision in trees:
        for path, oid in blobs(revision, changed_only=args.range is not None):
            # Path matters: the same blob can be approved in a deployment guide
            # but inappropriate as private benchmark evidence elsewhere.
            if (path, oid) in seen:
                continue
            seen.add((path, oid))
            blob = git("cat-file", "blob", oid)
            for line_number, line in enumerate(blob.decode(errors="replace").splitlines(), 1):
                category = problem(path, line, root)
                if category:
                    where = f" in {revision[:12]}" if args.range is not None else ""
                    print(f"{path}:{line_number}: {category}{where}; redact before publishing", file=sys.stderr)
                    problems += 1
    if problems:
        return 1
    print(f"Checked {len(seen)} documentation/benchmark blob versions: no local directory paths detected.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError:
        # Git errors can include local filenames; keep diagnostics path-free.
        print("Could not read Git evidence; check the repository and revision/history.", file=sys.stderr)
        sys.exit(2)
