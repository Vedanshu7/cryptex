#!/usr/bin/env python3
"""Validate a universe.json file before deploying it.

Run before committing or deploying an edit to pipeline/universe.json — the
services themselves fall back to the last-known-good universe on a bad file,
but that only helps once something is already known-good. This gives fast
feedback before that point.

Usage (from repo root):
    PYTHONPATH=pipeline python3 scripts/validate_universe.py [path/to/universe.json]

Exits non-zero if the file fails to parse or has any "error"-severity issue
(e.g. an unrecognized exchange name). "warning"-severity issues (an unusual
symbol notation) are printed but don't fail the check.
"""

import sys
from pathlib import Path

from pydantic import ValidationError

from shared.universe import Universe
from shared.universe_validation import validate_universe

_DEFAULT_PATH = "pipeline/universe.json"


def main() -> int:
    """Validate the given (or default) universe.json file. Returns exit code."""
    path = Path(sys.argv[1] if len(sys.argv) > 1 else _DEFAULT_PATH)

    try:
        universe = Universe.model_validate_json(path.read_text())
    except OSError as exc:
        print(f"Cannot read {path}: {exc}")
        return 1
    except ValidationError as exc:
        print(f"{path} is not a valid universe:\n{exc}")
        return 1

    issues = validate_universe(universe)
    if not issues:
        print(f"{path}: OK (version {universe.version}, {len(universe.regions)} region(s)).")
        return 0

    for issue in issues:
        print(f"[{issue.severity.upper()}] {issue.region}: {issue.message}")

    has_errors = any(issue.severity == "error" for issue in issues)
    if has_errors:
        print(f"\n{path}: FAILED — fix the error(s) above before deploying.")
        return 1

    print(f"\n{path}: passed with warning(s) above.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
