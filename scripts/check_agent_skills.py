#!/usr/bin/env python3
"""Validate installed project Rust Agent Skills without executing skill content."""
from pathlib import Path
import re
import sys

root = Path(__file__).resolve().parents[1] / ".agents" / "skills"
errors: list[str] = []
checked = 0
for manifest in sorted(root.glob("rust-*/SKILL.md")):
    source = manifest.read_text(encoding="utf-8")
    if not source.startswith("---\n") or "\n---\n" not in source[4:]:
        errors.append(f"{manifest.relative_to(root)}: missing YAML frontmatter")
        continue
    _, frontmatter, body = source.split("---\n", 2)
    fields = {}
    for line in frontmatter.splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            fields[key.strip()] = value.strip()
    if not fields.get("name") or not fields.get("description"):
        errors.append(f"{manifest.relative_to(root)}: name/description missing")
    if len(source.splitlines()) > 500:
        errors.append(f"{manifest.relative_to(root)}: exceeds 500 lines")
    for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", body):
        path = target.split("#", 1)[0]
        if not path or target.startswith(("http://", "https://", "mailto:")):
            continue
        if not (manifest.parent / path).resolve().exists():
            errors.append(f"{manifest.relative_to(root)}: broken local link {target}")
    checked += 1

if errors:
    print("\n".join(errors), file=sys.stderr)
    raise SystemExit(1)
print(f"Validated {checked} installed Rust skills: frontmatter, line limits and local links OK")
