#!/usr/bin/env bash
set -euo pipefail

html="${1:-web/trust-rotations.html}"
tmp="$(mktemp --suffix=.js)"
trap 'rm -f "$tmp"' EXIT
python3 - "$html" "$tmp" <<'PY'
from pathlib import Path
import re
import sys
source = Path(sys.argv[1]).read_text()
scripts = re.findall(r'<script>(.*?)</script>', source, re.S)
if len(scripts) != 1:
    raise SystemExit(f"expected exactly one inline script, found {len(scripts)}")
Path(sys.argv[2]).write_text(scripts[0])
PY
node --check "$tmp"
