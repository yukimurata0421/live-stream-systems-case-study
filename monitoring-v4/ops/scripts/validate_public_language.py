#!/usr/bin/env python3
"""Reject Japanese prose in public files; allow reviewed Unicode test inputs."""

from __future__ import annotations

import argparse
import ast
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tokenize

ROOT = Path(__file__).resolve().parents[2]
JAPANESE = re.compile(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]")
SCHEMA = "stream.public_language_policy.v1"


def line_digest(line: str) -> str:
    return hashlib.sha256(line.strip().encode("utf-8")).hexdigest()


def check_file(path: str, text: str, allowed: set[tuple[str, str]]) -> list[dict]:
    """An exception permits data, never a Python comment or docstring."""
    prose_lines: set[int] = set()
    if path.endswith(".py"):
        for token in tokenize.generate_tokens(io.StringIO(text).readline):
            if token.type == tokenize.COMMENT and JAPANESE.search(token.string):
                prose_lines.add(token.start[0])
        for node in ast.walk(ast.parse(text, filename=path)):
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                if ast.get_docstring(node, clean=False) and JAPANESE.search(ast.get_docstring(node, clean=False)):
                    doc = node.body[0]
                    prose_lines.update(range(doc.lineno, doc.end_lineno + 1))
    return [
        {"path": path, "line": number, "reason": "Japanese prose" if number in prose_lines else "Unreviewed Japanese text"}
        for number, line in enumerate(text.splitlines(), 1)
        if number in prose_lines
        or (JAPANESE.search(line) and (path, line_digest(line)) not in allowed)
    ]


def validate(root: Path) -> dict:
    policy = json.loads((root / "publication/language-policy.json").read_text(encoding="utf-8"))
    if policy.get("schema") != SCHEMA or policy.get("public_language") != "English":
        raise ValueError("PUBLIC_LANGUAGE_POLICY_INVALID")
    allowances = policy["unicode_data_exceptions"]
    allowed = {(row["path"], row["line_sha256"]) for row in allowances}
    if len(allowed) != len(allowances) or any(not row["reason"].strip() for row in allowances):
        raise ValueError("PUBLIC_LANGUAGE_EXCEPTION_INVALID")
    output = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "."], cwd=root
    )
    paths = sorted(set(output.decode("utf-8").split("\0")) - {""})
    errors: list[dict] = []
    seen: set[tuple[str, str]] = set()
    checked = 0
    for path in paths:
        file = root / path
        if not file.is_file() or file.is_symlink():
            continue
        raw = file.read_bytes()
        if b"\0" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        checked += 1
        errors.extend(check_file(path, text, allowed))
        seen.update((path, line_digest(line)) for line in text.splitlines() if JAPANESE.search(line))
    for path, digest in sorted(allowed - seen):
        errors.append({"path": path, "reason": "Stale Unicode data exception", "line_sha256": digest})
    return {"status": "FAIL" if errors else "PASS", "checked_text_files": checked,
            "unicode_data_exceptions": len(allowed), "errors": errors}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    result = validate(args.root.resolve())
    print(json.dumps(result, indent=2, sort_keys=True))
    return int(result["status"] != "PASS")


if __name__ == "__main__":
    raise SystemExit(main())
