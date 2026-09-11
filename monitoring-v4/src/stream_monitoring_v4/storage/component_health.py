from __future__ import annotations

from stream_contracts.monitoring_v4.time import utc_text


class ComponentHealthRepositoryMixin:
    def set_component_health(
        self,
        component: str,
        status: str,
        detail: str,
        *,
        now_ts: int,
    ) -> None:
        with self.connection() as connection:
            connection.execute(
                """INSERT INTO component_health(
                    component, status, checked_at, checked_ts, detail
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(component) DO UPDATE SET
                    status=excluded.status,
                    checked_at=excluded.checked_at,
                    checked_ts=excluded.checked_ts,
                    detail=excluded.detail
                WHERE excluded.checked_ts >= component_health.checked_ts""",
                (component, status, utc_text(now_ts), now_ts, detail[:500]),
            )
