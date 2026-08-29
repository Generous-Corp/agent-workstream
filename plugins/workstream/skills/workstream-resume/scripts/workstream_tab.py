#!/usr/bin/env python3
"""Forward to the packaged workstream tab-title runtime without PATH lookup."""

from pathlib import Path
import os
import sys


target = Path(__file__).resolve().parents[2] / "workstream-ledger/scripts/workstream_tab.py"
if not target.is_file():
    print("workstream_tab_runtime_unavailable", file=sys.stderr)
    raise SystemExit(2)
os.execv(sys.executable, [sys.executable, str(target), *sys.argv[1:]])
