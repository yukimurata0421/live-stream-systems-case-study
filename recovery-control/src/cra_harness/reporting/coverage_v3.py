from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from cra_harness.operational.model import AXES, HIGH_RISK_CELLS, OperationalScenario, matches_risk, total_possible_cells


def build_coverage(
    scenarios: list[OperationalScenario], outcomes: list[dict[str, Any]], *, minimum_high_risk_hits: int = 20
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    by_scenario = {str(item["scenario_id"]): item for item in outcomes}
    cells: dict[str, dict[str, Any]] = {}
    risk_scenarios: dict[str, list[str]] = defaultdict(list)
    for scenario in scenarios:
        outcome = by_scenario[scenario.scenario_id]
        cell = cells.setdefault(
            scenario.cell_id,
            {
                "cell_id": scenario.cell_id,
                "axes": scenario.cell,
                "hit_count": 0,
                "scenario_ids": [],
                "classification": {},
                "highest_risk": "NORMAL",
                "risk_ids": [],
            },
        )
        cell["hit_count"] += 1
        cell["scenario_ids"].append(scenario.scenario_id)
        counts = Counter(cell["classification"])
        counts[str(outcome["classification"])] += 1
        cell["classification"] = dict(sorted(counts.items()))
        for risk in HIGH_RISK_CELLS:
            if matches_risk(scenario, risk):
                risk_id = str(risk["risk_id"])
                risk_scenarios[risk_id].append(scenario.scenario_id)
                if risk_id not in cell["risk_ids"]:
                    cell["risk_ids"].append(risk_id)
                cell["highest_risk"] = "HIGH_RISK"
    risk_rows: list[dict[str, Any]] = []
    for risk in HIGH_RISK_CELLS:
        risk_id = str(risk["risk_id"])
        ids = sorted(set(risk_scenarios[risk_id]))
        risk_rows.append(
            {
                **risk,
                "minimum_hit_count": minimum_high_risk_hits,
                "hit_count": len(ids),
                "scenario_ids": ids,
                "covered": len(ids) >= minimum_high_risk_hits,
            }
        )
    possible = total_possible_cells()
    matrix = {
        "schema": "cra_harness.operational_state_matrix.v3",
        "axes": {key: list(value) for key, value in AXES.items()},
        "total_possible_modeled_cells": possible,
        "explored_cells": len(cells),
        "unexplored_cells": possible - len(cells),
        "scenario_hits": len(scenarios),
        "high_risk_cells": risk_rows,
        "high_risk_uncovered": sum(not row["covered"] for row in risk_rows),
        "cells": sorted(cells.values(), key=lambda item: str(item["cell_id"])),
        "coverage_interpretation": "full 7-axis product; only explored cells are materialized; absence is explicit, not assumed PASS",
    }
    uncovered = {
        "minimum_high_risk_hits": minimum_high_risk_hits,
        "uncovered_high_risk_cells": [row for row in risk_rows if not row["covered"]],
        "count": sum(not row["covered"] for row in risk_rows),
    }
    lines = [
        "# Operational State Space v3 Coverage",
        "",
        f"- Total possible modeled cells: {possible}",
        f"- Explored cells: {len(cells)}",
        f"- Unexplored cells: {possible - len(cells)}",
        f"- Scenario hits: {len(scenarios)}",
        f"- High-risk mandatory cells: {len(risk_rows)}",
        f"- High-risk uncovered: {uncovered['count']}",
        "",
        "## Mandatory high-risk coverage",
        "",
        "| risk | hit count | minimum | covered | title |",
        "| --- | ---: | ---: | --- | --- |",
    ]
    lines.extend(
        f"| {row['risk_id']} | {row['hit_count']} | {row['minimum_hit_count']} | {'yes' if row['covered'] else 'no'} | {row['title']} |"
        for row in risk_rows
    )
    lines.extend(
        [
            "",
            "全直積の未探索cellをPASSとは扱わない。JSON artifactは探索済みcellのidentity、hit count、",
            "scenario ID、classificationを保持する。",
            "",
        ]
    )
    return matrix, "\n".join(lines), uncovered
