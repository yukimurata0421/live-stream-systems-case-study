from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "public_language", Path(__file__).resolve().parents[1] / "ops/scripts/validate_public_language.py"
)
assert SPEC is not None and SPEC.loader is not None
LANGUAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LANGUAGE)
UNICODE_INPUT = "\u65e5\u672c\u8a9e"


def test_unicode_fixture_requires_exact_file_and_content() -> None:
    line = f'value = "{UNICODE_INPUT}"'
    allowance = {("fixture.py", LANGUAGE.line_digest(line))}
    assert LANGUAGE.check_file("fixture.py", line, allowance) == []
    assert LANGUAGE.check_file("other.py", line, allowance)
    assert LANGUAGE.check_file("fixture.py", line + ' + "extra"', allowance)


@pytest.mark.parametrize("line", [f"# {UNICODE_INPUT}", f'"""{UNICODE_INPUT}"""', '"""\\u65e5\\u672c\\u8a9e"""',
                                      f'def example():\n    """{UNICODE_INPUT}"""\n    pass'])
def test_exception_cannot_allow_comments_or_docstrings(line: str) -> None:
    allowance = {("fixture.py", LANGUAGE.line_digest(row)) for row in line.splitlines()}
    assert LANGUAGE.check_file("fixture.py", line, allowance)


def test_english_prose_and_unicode_escape_are_valid() -> None:
    assert LANGUAGE.check_file("example.py", '# English comment\nvalue = "\\u65e5"', set()) == []
