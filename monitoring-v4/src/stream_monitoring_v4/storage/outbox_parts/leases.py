from __future__ import annotations

from stream_contracts.monitoring_v4.time import utc_text


class LeaseRepositoryMixin:
    """Database lease and monotonically increasing fencing-token ownership."""

    def acquire_fenced_lease(
        self,
        name: str,
        owner: str,
        *,
        now_ts: int,
        ttl_sec: int,
    ) -> int | None:
        acquired_at = utc_text(now_ts)
        ttl = max(1, int(ttl_sec))
        expires_at = utc_text(now_ts + ttl)
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO leases(
                    lease_name, owner, acquired_at, acquired_ts, expires_at, expires_ts
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(lease_name) DO UPDATE SET
                    owner=excluded.owner, acquired_at=excluded.acquired_at,
                    acquired_ts=excluded.acquired_ts, expires_at=excluded.expires_at,
                    expires_ts=excluded.expires_ts
                WHERE leases.expires_ts <= ? OR leases.owner = ?""",
                (
                    name,
                    owner,
                    acquired_at,
                    now_ts,
                    expires_at,
                    now_ts + ttl,
                    now_ts,
                    owner,
                ),
            )
            row = connection.execute(
                "SELECT owner, expires_ts FROM leases WHERE lease_name=?",
                (name,),
            ).fetchone()
            if not row or row["owner"] != owner or int(row["expires_ts"]) <= now_ts:
                return None
            previous = connection.execute(
                "SELECT fence_token FROM lease_fences WHERE lease_name=?",
                (name,),
            ).fetchone()
            token = int(previous["fence_token"]) + 1 if previous else 1
            connection.execute(
                """INSERT INTO lease_fences(lease_name, fence_token) VALUES (?, ?)
                ON CONFLICT(lease_name) DO UPDATE SET fence_token=excluded.fence_token""",
                (name, token),
            )
            return token

    def acquire_lease(
        self,
        name: str,
        owner: str,
        *,
        now_ts: int,
        ttl_sec: int,
    ) -> bool:
        return (
            self.acquire_fenced_lease(
                name,
                owner,
                now_ts=now_ts,
                ttl_sec=ttl_sec,
            )
            is not None
        )

    def renew_fenced_lease(
        self,
        name: str,
        owner: str,
        fence_token: int,
        *,
        now_ts: int,
        ttl_sec: int,
    ) -> bool:
        ttl = max(1, int(ttl_sec))
        with self.transaction() as connection:
            fence = connection.execute(
                "SELECT fence_token FROM lease_fences WHERE lease_name=?",
                (name,),
            ).fetchone()
            if fence is None or int(fence["fence_token"]) != int(fence_token):
                return False
            cursor = connection.execute(
                """UPDATE leases SET expires_at=?, expires_ts=?
                WHERE lease_name=? AND owner=? AND expires_ts>?""",
                (utc_text(now_ts + ttl), now_ts + ttl, name, owner, now_ts),
            )
            return cursor.rowcount == 1

    def release_lease(
        self,
        name: str,
        owner: str,
        fence_token: int | None = None,
    ) -> None:
        with self.transaction() as connection:
            if fence_token is not None:
                fence = connection.execute(
                    "SELECT fence_token FROM lease_fences WHERE lease_name=?",
                    (name,),
                ).fetchone()
                if fence is None or int(fence["fence_token"]) != int(fence_token):
                    return
            connection.execute(
                """UPDATE leases SET expires_at='', expires_ts=0
                WHERE lease_name=? AND owner=?""",
                (name, owner),
            )
