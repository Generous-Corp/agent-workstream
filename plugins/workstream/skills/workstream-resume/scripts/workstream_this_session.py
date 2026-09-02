#!/usr/bin/env python3
"""Execute the plugin's exact-terminal same-session resume implementation."""

from __future__ import annotations

import os
from pathlib import Path
import stat
import sys


def main() -> int:
    skills_root = Path(__file__).resolve().parents[2]
    candidate = (
        skills_root / "workstream-ledger/scripts/workstream_this_session.py"
    )
    try:
        target = candidate.resolve(strict=True)
        target.relative_to(skills_root.resolve(strict=True))
        if not stat.S_ISREG(target.stat().st_mode) or candidate.is_symlink():
            raise ValueError("not a regular bundled file")
    except (OSError, ValueError) as error:
        print(f"workstream_this_session_runtime_unavailable:{error}", file=sys.stderr)
        return 2
    os.execv(sys.executable, [sys.executable, str(target), *sys.argv[1:]])
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
