from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator


JST = timezone(timedelta(hours=9))
RETENTION_DAYS = 400
SCHEMA_VERSION = 1


def _hash(value: object) -> str:
    text = str(value or "").strip()
    return hashlib.sha256(text.encode("utf-8")).hexdigest() if text else ""


def _parse_ts(value: object) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


@contextmanager
def connect(path: Path) -> Iterator[sqlite3.Connection]:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=10)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS same_url_transition_ledger (
          event_id TEXT PRIMARY KEY,
          observed_ts INTEGER NOT NULL,
          observed_day_jst TEXT NOT NULL,
          event_type TEXT NOT NULL,
          live_url_sha256 TEXT NOT NULL,
          previous_live_url_sha256 TEXT NOT NULL,
          video_id_sha256 TEXT NOT NULL,
          expected_video_id_sha256 TEXT NOT NULL,
          controlled INTEGER,
          candidate_new_url INTEGER NOT NULL,
          force_live_triggered INTEGER NOT NULL,
          source TEXT NOT NULL,
          revision TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS same_url_transition_observed_idx
          ON same_url_transition_ledger(observed_ts);
        CREATE INDEX IF NOT EXISTS same_url_transition_daily_idx
          ON same_url_transition_ledger(event_type, observed_day_jst, live_url_sha256);
        CREATE TABLE IF NOT EXISTS sli_rollups (
          granularity TEXT NOT NULL,
          bucket_start_ts INTEGER NOT NULL,
          bucket_end_ts INTEGER NOT NULL,
          family TEXT NOT NULL,
          official_window TEXT NOT NULL,
          compliance_status TEXT NOT NULL,
          measurement_status TEXT NOT NULL,
          sli_pct REAL,
          coverage_pct REAL,
          source_freshness_pct REAL,
          good_units REAL,
          bad_units REAL,
          missing_units REAL,
          payload_json TEXT NOT NULL,
          revision TEXT NOT NULL,
          generated_at_ts INTEGER NOT NULL,
          PRIMARY KEY(granularity, bucket_start_ts, family)
        );
        CREATE INDEX IF NOT EXISTS sli_rollups_generated_idx ON sli_rollups(generated_at_ts);
        """
    )
    db.execute(
        "INSERT OR IGNORE INTO metadata(key, value) VALUES('schema_version', ?)",
        (str(SCHEMA_VERSION),),
    )
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def record_same_url_event(db_path: Path, payload: dict, *, revision: str = "") -> bool:
    observed_ts = _parse_ts(payload.get("ts_utc"))
    event_id = str(payload.get("event_id") or "").strip()
    if observed_ts <= 0 or not event_id:
        return False
    live_hash = _hash(payload.get("live_url"))
    candidate = bool(payload.get("candidate_new_url_found"))
    forced = bool(payload.get("force_live_triggered"))
    with connect(db_path) as db:
        if db.execute(
            "SELECT 1 FROM same_url_transition_ledger WHERE event_id = ?",
            (event_id,),
        ).fetchone():
            return False
        day_jst = datetime.fromtimestamp(observed_ts, JST).date().isoformat()
        previous = db.execute(
            "SELECT live_url_sha256, observed_day_jst FROM same_url_transition_ledger "
            "WHERE live_url_sha256 != '' AND observed_ts < ? "
            "ORDER BY observed_ts DESC LIMIT 1",
            (observed_ts,),
        ).fetchone()
        previous_hash = str(previous[0]) if previous else ""
        if live_hash and previous_hash and live_hash != previous_hash:
            event_type = "url_transition"
        elif not previous_hash and live_hash:
            event_type = "baseline"
        elif candidate:
            event_type = "candidate_new_url"
        elif forced:
            event_type = "force_live_attempt"
        elif live_hash:
            daily_exists = db.execute(
                """
                SELECT 1 FROM same_url_transition_ledger
                WHERE observed_day_jst = ? AND live_url_sha256 = ?
                  AND event_type IN ('baseline', 'daily_checkpoint')
                LIMIT 1
                """,
                (day_jst, live_hash),
            ).fetchone()
            if daily_exists:
                return False
            event_type = "daily_checkpoint"
        else:
            return False
        controlled_value = payload.get("controlled_transition")
        controlled = None if controlled_value is None else int(bool(controlled_value))
        cursor = db.execute(
            """
            INSERT OR IGNORE INTO same_url_transition_ledger(
              event_id, observed_ts, observed_day_jst, event_type,
              live_url_sha256, previous_live_url_sha256, video_id_sha256,
              expected_video_id_sha256, controlled, candidate_new_url,
              force_live_triggered, source, revision
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                event_id,
                observed_ts,
                day_jst,
                event_type,
                live_hash,
                previous_hash,
                _hash(payload.get("video_id")),
                _hash(payload.get("expected_video_id")),
                controlled,
                int(candidate),
                int(forced),
                "youtube_watchdog",
                revision,
            ),
        )
        return cursor.rowcount > 0


def backfill_same_url_events(db_path: Path, events: Iterable[dict], *, revision: str = "") -> int:
    ordered = sorted(
        (payload for payload in events if isinstance(payload, dict)),
        key=lambda payload: _parse_ts(payload.get("ts_utc")),
    )
    if not ordered:
        return 0
    first_ts = _parse_ts(ordered[0].get("ts_utc"))
    inserted = 0
    with connect(db_path) as db:
        existing_ids = {
            str(row[0])
            for row in db.execute("SELECT event_id FROM same_url_transition_ledger")
        }
        daily_keys = {
            (str(row[0]), str(row[1]))
            for row in db.execute(
                """
                SELECT observed_day_jst, live_url_sha256
                FROM same_url_transition_ledger
                WHERE event_type IN ('baseline', 'daily_checkpoint')
                """
            )
        }
        previous_row = db.execute(
            """
            SELECT live_url_sha256 FROM same_url_transition_ledger
            WHERE live_url_sha256 != '' AND observed_ts < ?
            ORDER BY observed_ts DESC LIMIT 1
            """,
            (first_ts,),
        ).fetchone()
        last_live_hash = str(previous_row[0]) if previous_row else ""
        for payload in ordered:
            observed_ts = _parse_ts(payload.get("ts_utc"))
            event_id = str(payload.get("event_id") or "").strip()
            live_hash = _hash(payload.get("live_url"))
            if observed_ts <= 0 or not event_id:
                continue
            previous_hash = last_live_hash
            day_jst = datetime.fromtimestamp(observed_ts, JST).date().isoformat()
            candidate = bool(payload.get("candidate_new_url_found"))
            forced = bool(payload.get("force_live_triggered"))
            event_type = ""
            if event_id not in existing_ids:
                if live_hash and previous_hash and live_hash != previous_hash:
                    event_type = "url_transition"
                elif not previous_hash and live_hash:
                    event_type = "baseline"
                elif candidate:
                    event_type = "candidate_new_url"
                elif forced:
                    event_type = "force_live_attempt"
                elif live_hash and (day_jst, live_hash) not in daily_keys:
                    event_type = "daily_checkpoint"
            if event_type:
                controlled_value = payload.get("controlled_transition")
                controlled = None if controlled_value is None else int(bool(controlled_value))
                cursor = db.execute(
                    """
                    INSERT OR IGNORE INTO same_url_transition_ledger(
                      event_id, observed_ts, observed_day_jst, event_type,
                      live_url_sha256, previous_live_url_sha256, video_id_sha256,
                      expected_video_id_sha256, controlled, candidate_new_url,
                      force_live_triggered, source, revision
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        event_id,
                        observed_ts,
                        day_jst,
                        event_type,
                        live_hash,
                        previous_hash,
                        _hash(payload.get("video_id")),
                        _hash(payload.get("expected_video_id")),
                        controlled,
                        int(candidate),
                        int(forced),
                        "youtube_watchdog_backfill",
                        revision,
                    ),
                )
                inserted += int(cursor.rowcount > 0)
                existing_ids.add(event_id)
                if event_type in {"baseline", "daily_checkpoint"}:
                    daily_keys.add((day_jst, live_hash))
            if live_hash:
                last_live_hash = live_hash
    return inserted


def compact_duplicate_daily_checkpoints(db_path: Path) -> int:
    """Keep one daily checkpoint per JST day and URL hash.

    Transition, candidate, force and baseline evidence is never removed.
    """
    with connect(db_path) as db:
        before = db.total_changes
        db.execute(
            """
            DELETE FROM same_url_transition_ledger AS daily
            WHERE daily.event_type = 'daily_checkpoint'
              AND EXISTS (
                SELECT 1 FROM same_url_transition_ledger AS baseline
                WHERE baseline.event_type = 'baseline'
                  AND baseline.observed_day_jst = daily.observed_day_jst
                  AND baseline.live_url_sha256 = daily.live_url_sha256
              )
            """
        )
        db.execute(
            """
            DELETE FROM same_url_transition_ledger
            WHERE event_type = 'daily_checkpoint'
              AND rowid NOT IN (
                SELECT MIN(rowid)
                FROM same_url_transition_ledger
                WHERE event_type = 'daily_checkpoint'
                GROUP BY observed_day_jst, live_url_sha256
              )
            """
        )
        return db.total_changes - before


def upsert_rollups(
    db_path: Path,
    report: dict,
    *,
    granularity: str,
    bucket_start_ts: int,
    bucket_end_ts: int,
    window_label: str,
    revision: str,
) -> int:
    window = report.get("windows", {}).get(window_label, {})
    generated_at_ts = _parse_ts(report.get("generated_at_utc"))
    rows = 0
    with connect(db_path) as db:
        for family, item in window.items():
            measurement = item.get("selected") if family == "youtube_availability" else item
            if family == "same_url_preservation":
                measurement = item.get("raw_metric")
            if not isinstance(measurement, dict):
                continue
            db.execute(
                """
                INSERT OR REPLACE INTO sli_rollups(
                  granularity,bucket_start_ts,bucket_end_ts,family,official_window,
                  compliance_status,measurement_status,sli_pct,coverage_pct,
                  source_freshness_pct,good_units,bad_units,missing_units,payload_json,
                  revision,generated_at_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    granularity,
                    bucket_start_ts,
                    bucket_end_ts,
                    family,
                    str(report.get("policies", {}).get(family, {}).get("official_window", "")),
                    str(measurement.get("compliance_status", "unknown")),
                    str(measurement.get("measurement_status", "unknown")),
                    measurement.get("sli_pct"),
                    measurement.get("coverage_pct"),
                    measurement.get("source_freshness_pct"),
                    measurement.get("good_points"),
                    measurement.get("bad_points"),
                    measurement.get("missing_points"),
                    json.dumps(item, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                    revision,
                    generated_at_ts,
                ),
            )
            rows += 1
    return rows


def prune(db_path: Path, *, now_ts: int, retention_days: int = RETENTION_DAYS) -> dict[str, int]:
    cutoff = int(now_ts) - max(1, int(retention_days)) * 86400
    removed: dict[str, int] = {}
    with connect(db_path) as db:
        for table, column in (
            ("same_url_transition_ledger", "observed_ts"),
            ("sli_rollups", "bucket_end_ts"),
        ):
            before = db.total_changes
            db.execute(f"DELETE FROM {table} WHERE {column} < ?", (cutoff,))
            removed[table] = db.total_changes - before
    return removed


def counts(db_path: Path) -> dict[str, int]:
    with connect(db_path) as db:
        return {
            "same_url_transition_ledger": int(
                db.execute("SELECT COUNT(*) FROM same_url_transition_ledger").fetchone()[0]
            ),
            "sli_rollups": int(db.execute("SELECT COUNT(*) FROM sli_rollups").fetchone()[0]),
        }


def same_url_ledger_evidence(db_path: Path, *, start_ts: int, end_ts: int) -> dict:
    if not db_path.exists():
        return {"available": False, "row_count": 0}
    with connect(db_path) as db:
        rows = db.execute(
            """
            SELECT observed_ts, observed_day_jst, event_type, controlled,
                   live_url_sha256, previous_live_url_sha256
            FROM same_url_transition_ledger
            WHERE observed_ts >= ? AND observed_ts <= ?
            ORDER BY observed_ts
            """,
            (start_ts, end_ts),
        ).fetchall()
    days = {str(row[1]) for row in rows if row[1]}
    expected_days = max(1, (end_ts - start_ts + 86399) // 86400)
    transitions = [row for row in rows if row[2] == "url_transition"]
    return {
        "available": bool(rows),
        "row_count": len(rows),
        "distinct_checkpoint_days": len(days),
        "expected_days": expected_days,
        "coverage_pct": round(len(days) / expected_days * 100.0, 3),
        "transition_count": len(transitions),
        "uncontrolled_transition_count": sum(row[3] == 0 for row in transitions),
        "unclassified_transition_count": sum(row[3] is None for row in transitions),
        "first_observed_ts": int(rows[0][0]) if rows else 0,
        "last_observed_ts": int(rows[-1][0]) if rows else 0,
        "plain_url_retained": False,
    }
