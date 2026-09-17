"""
FiniexDataCollector - Documentation Link Tests

A link in a document is a claim that a file is there under that name. On Windows
and macOS the filesystem answers to any spelling, so `README.md` resolved to
`readme.md` for months - in the editor, in the dev container (whose /app is a
Windows bind mount), everywhere anybody looked. It broke in the two places that
are case-sensitive and that nobody looks at until they fail: GitHub's own link
resolution, and CI on a real Linux filesystem.

That is why this compares against the directory listing rather than asking
`Path.exists()`. The question is not "can this be opened here" but "is this the
name", and only one of the two travels.

Location: tests/utils/test_doc_links.py
"""

import os
import re
from pathlib import Path
from typing import List, Set, Tuple

# `[text](target)`, target captured. Titles and anchors are trimmed afterwards.
MARKDOWN_LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")

# Documents a reader is expected to follow. `github_issues/` is a gitignored
# snapshot of somebody else's prose and is not ours to keep consistent.
DOC_ROOTS = ("docs", ".")

SKIP_PREFIXES = ("http://", "https://", "mailto:", "#")


def tracked_markdown() -> List[Path]:
    """
    Every markdown document this project maintains.

    Returns:
        Paths relative to the project root
    """
    found: List[Path] = []
    for path in Path("docs").rglob("*.md"):
        found.append(path)
    for path in Path(".").glob("*.md"):
        found.append(path)
    return sorted(found)


def link_targets(document: Path) -> List[str]:
    """
    Relative link targets in one document.

    Args:
        document: The markdown file to read

    Returns:
        Targets, with anchors and titles removed, external ones dropped
    """
    targets = []
    for raw in MARKDOWN_LINK.findall(document.read_text(encoding="utf-8")):
        target = raw.split(" ")[0].split("#")[0].strip()
        if not target or target.startswith(SKIP_PREFIXES):
            continue
        targets.append(target)
    return targets


def exists_with_this_exact_name(path: Path) -> bool:
    """
    Whether the path exists spelled exactly this way.

    `Path.exists()` cannot answer this on Windows or macOS: it resolves any
    spelling, which is precisely how a broken link survives every local check
    and fails on GitHub.

    Args:
        path: The path to verify, as written in the link

    Returns:
        True when a directory entry carries exactly this name
    """
    parent = path.parent
    if not parent.is_dir():
        return False

    return path.name in {entry.name for entry in parent.iterdir()}


def test_every_documentation_link_resolves_case_sensitively() -> None:
    """
    A link that works only on a case-insensitive filesystem is a broken link.

    It cost this project two of them at once, both found by CI rather than by
    reading: the version guard opened `README.md` while the file is `readme.md`,
    and the documentation index pointed at the same wrong spelling - which means
    it had been dead on GitHub the whole time.
    """
    broken: Set[Tuple[str, str]] = set()

    for document in tracked_markdown():
        for target in link_targets(document):
            # `normpath` collapses `..` lexically, without asking the filesystem.
            # `Path.resolve()` cannot be used here: on Windows it silently returns
            # the real on-disk spelling, so it repairs the very mistake this test
            # exists to find - which it did, on the first run.
            joined = os.path.normpath(os.path.join(str(document.parent), target))
            relative = Path(joined)
            if relative.is_absolute() or joined.startswith(".."):
                # Points outside the project; not ours to verify.
                continue
            if not exists_with_this_exact_name(relative):
                broken.add((str(document), target))

    assert not broken, "links pointing at nothing, or at another spelling:\n" + \
        "\n".join(f"  {doc} -> {target}" for doc, target in sorted(broken))
