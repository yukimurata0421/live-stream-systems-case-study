from __future__ import annotations

from itertools import product

from cra_harness.covering_array import coverage_report, generate_covering_array
from cra_harness.runner.external_boundary_covering import _mandatory_rows


def test_external_boundary_mixed_rows_are_unique_and_complete() -> None:
    rows = _mandatory_rows()

    assert len(rows) == len(set(rows)) == 3456
    assert {row[0] for row in rows} == set(range(8))
    assert {row[1] for row in rows} == {1, 2, 3, 5}
    assert {row[8] for row in rows} == {0, 1, 2}


def test_small_external_boundary_array_has_independent_complete_coverage() -> None:
    sizes = (5, 4, 3, 3, 2, 2)
    mandatory = tuple((*values, 0, 0, 0) for values in product(range(2), range(2), range(2)))
    rows = generate_covering_array(sizes, strength=3, target_count=256, mandatory_rows=mandatory, seed=20260902)
    report = coverage_report(rows, sizes, strength=3)

    assert len(rows) == len(set(rows)) == 256
    assert set(mandatory) <= set(rows)
    assert report["coverage_complete"] is True
    assert report["missing_combination_count"] == 0
