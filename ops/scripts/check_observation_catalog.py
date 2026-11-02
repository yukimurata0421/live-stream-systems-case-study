#!/usr/bin/env python3
"""Validate policy ownership and migration acceptance without changing runtime values."""
import argparse
import ast
from datetime import date
import json
from pathlib import Path


def check(root):
    folder = root / "configs/observation"
    policy = json.loads((folder / "policy-catalog.json").read_text())
    retirement = json.loads((folder / "retirement-plan.json").read_text())
    assert policy["schema"] == "stream.observation_policy_catalog.v1"
    assert retirement["schema"] == "stream.observation_retirement_plan.v1"
    assert retirement["review_dates_are_not_deletion_authority"] is True
    for entries in (policy["entries"], retirement["entries"]):
        assert len({x["id"] for x in entries}) == len(entries)
        assert all(x.get("owner") for x in entries)
    for item in policy["entries"]:
        assert item.get("basis") and item.get("unit")
        source = item["source"]
        p = (root / source["path"]).resolve()
        assert p.is_relative_to(root.resolve()) and p.is_file()
        if "symbol" in source:
            values = {}
            for node in ast.parse(p.read_text()).body:
                if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                    try:
                        values[node.targets[0].id] = ast.literal_eval(node.value)
                    except (ValueError, TypeError):
                        pass
            assert values[source["symbol"]] == item["value"], item["id"] + ": policy/source drift"
    for item in retirement["entries"]:
        assert item["state"] in {"pending", "removal_candidate", "retained_by_design", "disabled"}
        assert item.get("consumers") is not None and item.get("reason") and item.get("acceptance")
        date.fromisoformat(item["review_due_jst"])
    return {"policy_entries": len(policy["entries"]), "route_entries": len(retirement["entries"]), "status": "PASS"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[2])
    print(json.dumps(check(parser.parse_args().repo)))
