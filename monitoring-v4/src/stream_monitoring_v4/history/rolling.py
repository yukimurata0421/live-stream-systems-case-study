from __future__ import annotations

from typing import Any

from stream_contracts.monitoring_v4.ids import stable_id

STATUSES = ('good', 'bad', 'unknown', 'not_applicable')
WINDOWS = (3600, 86400, 7 * 86400)
DEFINITION = 'received-cycle-availability.v1'
CORRECTION_LIMIT_SEC = 7 * 86400


def series_identity(metric: str, target: str, definition_revision: str) -> str:
    return stable_id('ser', metric, target, definition_revision)


def _counts(connection: Any, series: str, start: int, end: int) -> list[int]:
    row = connection.execute(
        "SELECT " + ', '.join(
            f"COALESCE(SUM(CASE WHEN status='{s}' THEN 1 ELSE 0 END), 0) AS {s}"
            for s in STATUSES
        ) + " FROM metric_minutes WHERE series_id=? AND minute_ts>=? AND minute_ts<?",
        (series, start, end),
    ).fetchone()
    return [int(row[s]) for s in STATUSES]


def advance(connection: Any, series: str, *, end_ts: int) -> None:
    """Advance the durable checkpoint and totals in the caller's transaction."""
    end = end_ts // 60 * 60
    for window in WINDOWS:
        row = connection.execute(
            'SELECT * FROM metric_windows WHERE series_id=? AND window_sec=?',
            (series, window),
        ).fetchone()
        if row is None:
            totals = _counts(connection, series, end - window, end)
        else:
            previous = int(row['end_ts'])
            if end < previous:
                raise ValueError('rolling clock regressed')
            if end == previous:
                continue
            if end - previous >= window:
                totals = _counts(connection, series, end - window, end)
            else:
                added = _counts(connection, series, previous, end)
                removed = _counts(connection, series, previous - window, end - window)
                totals = [int(row[s]) + a - r for s, a, r in zip(STATUSES, added, removed)]
        connection.execute(
            '''INSERT INTO metric_windows(series_id, window_sec, end_ts, good, bad, unknown,
                not_applicable) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(series_id, window_sec) DO UPDATE SET end_ts=excluded.end_ts,
                good=excluded.good, bad=excluded.bad, unknown=excluded.unknown,
                not_applicable=excluded.not_applicable''',
            (series, window, end, *totals),
        )


def record_slot(connection: Any, *, metric: str, target: str, sample_ts: int,
                event_id: str, status: str, now_ts: int,
                definition_revision: str = DEFINITION) -> bool:
    """One scheduled fact per minute; a newer sample replaces its contribution.

    This counts received-cycle availability/current state, never API attempts or
    OCR samples. Cached facts are valid only after the caller checks freshness.
    No build/restart identity is part of the series key. Late replacement keeps
    its original minute and adjusts all windows containing it exactly once.
    """
    if status not in STATUSES or not all((metric, target, definition_revision, event_id)):
        raise ValueError('invalid metric slot')
    if sample_ts > now_ts or sample_ts < now_ts - CORRECTION_LIMIT_SEC:
        raise ValueError('sample outside the normal correction interval; use offline reprocessing')
    series = series_identity(metric, target, definition_revision)
    minute = sample_ts // 60 * 60
    connection.execute(
        '''INSERT INTO metric_series(series_id, metric, target, definition_revision, started_ts)
           VALUES (?, ?, ?, ?, ?) ON CONFLICT(series_id) DO NOTHING''',
        (series, metric, target, definition_revision, minute),
    )
    started = connection.execute('SELECT started_ts FROM metric_series WHERE series_id=?', (series,)).fetchone()
    if minute < int(started['started_ts']):
        raise ValueError('sample precedes measurement start; use explicit offline reprocessing')
    old = connection.execute(
        'SELECT * FROM metric_minutes WHERE series_id=? AND minute_ts=?', (series, minute)
    ).fetchone()
    if old is not None:
        if old['event_id'] == event_id:
            if old['status'] != status or int(old['sample_ts']) != sample_ts:
                raise ValueError('conflicting metric event identity')
            advance(connection, series, end_ts=now_ts)
            return False
        if (sample_ts, event_id) <= (int(old['sample_ts']), old['event_id']):
            advance(connection, series, end_ts=now_ts)
            return False
    connection.execute(
        '''INSERT INTO metric_minutes(series_id, minute_ts, sample_ts, event_id, status)
           VALUES (?, ?, ?, ?, ?) ON CONFLICT(series_id, minute_ts) DO UPDATE SET
           sample_ts=excluded.sample_ts, event_id=excluded.event_id, status=excluded.status''',
        (series, minute, sample_ts, event_id, status),
    )
    # Correct already-accounted buckets before advancing the same checkpoint.
    for window in WINDOWS:
        row = connection.execute(
            'SELECT end_ts FROM metric_windows WHERE series_id=? AND window_sec=?',
            (series, window),
        ).fetchone()
        if row is not None and int(row['end_ts']) - window <= minute < int(row['end_ts']):
            delta = [int(status == s) - int(old is not None and old['status'] == s) for s in STATUSES]
            connection.execute(
                '''UPDATE metric_windows SET good=good+?, bad=bad+?, unknown=unknown+?,
                   not_applicable=not_applicable+? WHERE series_id=? AND window_sec=?''',
                (*delta, series, window),
            )
    advance(connection, series, end_ts=now_ts)
    return True


def read_windows(connection: Any, *, target: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        '''SELECT s.metric, s.definition_revision, s.started_ts, w.* FROM metric_windows w
           JOIN metric_series s ON s.series_id=w.series_id WHERE s.target=?
           ORDER BY s.metric, w.window_sec''', (target,),
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        start = max(int(row['end_ts']) - int(row['window_sec']), int(row['started_ts']))
        expected = max(0, (int(row['end_ts']) - start) // 60)
        observed = sum(int(row[s]) for s in STATUSES)
        # Late slots before measurement start remain diagnostic, not denominators.
        missing = max(0, expected - observed)
        known = int(row['good']) + int(row['bad'])
        eligible = expected - int(row['not_applicable'])
        elapsed = int(row['end_ts']) - int(row['started_ts']) >= int(row['window_sec'])
        item.update(expected=expected, observed=observed, missing=missing,
                    window_elapsed=elapsed, collection_status='WINDOW_ELAPSED' if elapsed else 'COLLECTING',
                    coverage_pct=100 * observed / expected if expected else None,
                    good_pct_known=100 * int(row['good']) / known if known else None,
                    good_pct_conservative=100 * int(row['good']) / eligible if eligible else None)
        result.append(item)
    return result
