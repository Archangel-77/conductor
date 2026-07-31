#!/usr/bin/env python3
"""Extract the release notes for the current tag from ``CHANGELOG.md``.

Reads the ``GITHUB_REF_NAME`` environment variable (e.g. ``v0.1.0``) and
writes the matching ``## [X.Y.Z]`` section to ``release_body.md``.  Falls
back to the whole changelog if the version section is not found.

Used by ``.github/workflows/release.yml`` so the GitHub Release body
mirrors the CHANGELOG instead of auto-generated commit notes.
"""

from __future__ import annotations

import os
import pathlib
import re
import sys


def extract_section(changelog: str, version: str) -> str:
    """Return the changelog section for *version* (no leading ``v``)."""
    pattern = rf"^## \[{re.escape(version)}\] - .*?(?=^## \[|\Z)"
    match = re.search(pattern, changelog, flags=re.MULTILINE | re.DOTALL)
    return match.group(0).strip() if match else changelog.strip()


def main() -> int:
    tag = os.environ.get("GITHUB_REF_NAME", "v0.1.0")
    version = tag[1:] if tag.startswith("v") else tag
    changelog_path = pathlib.Path("CHANGELOG.md")
    if not changelog_path.exists():
        print("CHANGELOG.md not found", file=sys.stderr)
        return 1
    body = extract_section(changelog_path.read_text(encoding="utf-8"), version)
    pathlib.Path("release_body.md").write_text(body + "\n", encoding="utf-8")
    print(f"Extracted release notes for {tag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
