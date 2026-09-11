from __future__ import annotations

import json
from typing import Any


class MonitoringMetricsRepositoryMixin:
    """Backend-neutral aggregate queries needed by the Prometheus exporter."""

    def monitoring_metrics_snapshot(self) -> dict[str, Any]:
        with self.connection(read_only=True) as connection:
            rejection_count = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM rejections"
                ).fetchone()["count"]
            )
            cycle_count = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM shadow_cycles"
                ).fetchone()["count"]
            )
            transition_rows = connection.execute(
                "SELECT domain, phase, COUNT(*) AS count "
                "FROM incident_transitions GROUP BY domain, phase"
            ).fetchall()
            latest_parity = connection.execute(
                "SELECT parity_json FROM shadow_cycles "
                "ORDER BY started_ts DESC, cycle_id DESC LIMIT 1"
            ).fetchone()
            publication_rows = connection.execute(
                "SELECT state, COUNT(*) AS count "
                "FROM public_artifact_publications GROUP BY state"
            ).fetchall()
        return {
            "rejection_count": rejection_count,
            "cycle_count": cycle_count,
            "transition_counts": [dict(row) for row in transition_rows],
            "latest_parity": (
                json.loads(latest_parity["parity_json"]) if latest_parity else None
            ),
            "publication_counts": {
                str(row["state"]): int(row["count"])
                for row in publication_rows
            },
        }
