from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import json
import os
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

from stream_contracts.monitoring_v4.ids import canonical_json
from stream_monitoring_v4.runtime.atomic_file import atomic_write_text
from .snapshot import read_snapshot, streamed

# Only append/copy. There is deliberately no source DELETE/UPDATE path.
# Day clocks are received time for observations/rejections; other tables carry
# their own explicit creation/reduction/evaluation clock, never observed time.
TABLES = {
    'observations': ('observation_id', 'received_ts'),
    'rejections': ('rejection_id', 'received_ts'),
    'shadow_cycles': ('cycle_id', 'completed_ts'),
    'current_snapshots': ('snapshot_id', 'reduced_ts'),
    'sli_projections': ('projection_id', 'evaluated_ts'),
}
JST = dt.timezone(dt.timedelta(hours=9))


def day_bounds(day: str) -> tuple[int, int]:
    date = dt.date.fromisoformat(day)
    if date.isoformat() != day:
        raise ValueError('day must be YYYY-MM-DD in JST')
    start = int(dt.datetime.combine(date, dt.time(), JST).timestamp())
    return start, start + 86400


def _sync_file(path: Path) -> None:
    with path.open('rb') as handle:
        os.fsync(handle.fileno())


def _sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def verify_archive(path: Path) -> dict:
    """Reopen read-only; verify rows, original identity and content, not just size."""
    with closing(sqlite3.connect(f'{path.resolve().as_uri()}?mode=ro&immutable=1', uri=True)) as check:
        if check.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise RuntimeError('archive SQLite integrity failure')
        manifest = json.loads(check.execute('SELECT payload FROM manifest').fetchone()[0])
        if manifest['schema'] != 'monitoring_v4.history_archive.v1':
            raise RuntimeError('unsupported archive schema')
        total = 0
        for table, expected in manifest['tables'].items():
            digest = hashlib.sha256()
            count = 0
            cursor = check.execute('SELECT row_id, row_json FROM records WHERE table_name=? ORDER BY ordinal', (table,))
            while rows := cursor.fetchmany(128):
                for identity, payload in rows:
                    row = json.loads(payload)
                    if identity != str(row[expected['identity_column']]):
                        raise RuntimeError('archive row identity mismatch')
                    digest.update(payload.encode()); digest.update(b'\n')
                    count += 1
            if count != expected['count'] or digest.hexdigest() != expected['sha256']:
                raise RuntimeError('archive content mismatch')
            total += count
        if check.execute('SELECT COUNT(*) FROM records').fetchone()[0] != total:
            raise RuntimeError('unexpected archive rows')
        return manifest


def archive_day(repository, *, day: str, output_dir: Path, now_ts: int,
                max_bytes: int = 512 * 1024**2) -> dict:
    start, end = day_bounds(day)
    if end > now_ts - 7 * 86400:
        raise ValueError('copy only completed JST days older than seven days')
    if max_bytes < 4096:
        raise ValueError('archive byte budget too small')
    root = output_dir.resolve()
    source_path = getattr(repository, 'path', None)
    if source_path is not None and (root == source_path.resolve() or root in source_path.resolve().parents):
        raise ValueError('archive directory must be separate from source database directory')
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock_fd = os.open(root / '.writer.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return _copy_locked(repository, day=day, root=root, start=start, end=end, max_bytes=max_bytes)
    finally:
        os.close(lock_fd)


def _copy_locked(repository, *, day, root, start, end, max_bytes):
    fd, name = tempfile.mkstemp(prefix=f'.history-{day}-', suffix='.staging', dir=root)
    os.close(fd)
    staging = Path(name)
    try:
        manifest = {'schema': 'monitoring_v4.history_archive.v1', 'day_jst': day,
                    'start_ts': start, 'end_ts': end, 'copy_only': True, 'tables': {}}
        copied_bytes = 0
        with closing(sqlite3.connect(staging)) as dest, read_snapshot(repository) as source:
            dest.execute('PRAGMA synchronous=FULL')
            dest.execute('''CREATE TABLE records(table_name TEXT NOT NULL, row_id TEXT NOT NULL,
                         ordinal INTEGER NOT NULL, row_json TEXT NOT NULL,
                         PRIMARY KEY(table_name, row_id), UNIQUE(table_name, ordinal))''')
            dest.execute('CREATE TABLE manifest(payload TEXT NOT NULL)')
            for table, (identity, clock) in TABLES.items():
                digest = hashlib.sha256(); count = 0
                with streamed(source, f'SELECT * FROM {table} WHERE {clock}>=? AND {clock}<? ORDER BY {clock}, {identity}', (start, end)) as cursor:
                    while rows := cursor.fetchmany(128):
                        for record in rows:
                            row = dict(record)
                            payload = canonical_json(row)
                            copied_bytes += len(payload.encode())
                            if copied_bytes > max_bytes:
                                raise RuntimeError('archive byte budget exceeded; source retained')
                            dest.execute('INSERT INTO records VALUES (?, ?, ?, ?)', (table, str(row[identity]), count, payload))
                            digest.update(payload.encode()); digest.update(b'\n'); count += 1
                manifest['tables'][table] = {'count': count, 'sha256': digest.hexdigest(),
                                             'identity_column': identity, 'partition_clock': clock}
            # Store metric buckets by UTC minute with definitions/measurement start.
            digest = hashlib.sha256(); count = 0
            with streamed(source, '''SELECT m.*, s.metric, s.target, s.definition_revision, s.started_ts
                         FROM metric_minutes m JOIN metric_series s ON m.series_id=s.series_id
                         WHERE m.minute_ts>=? AND m.minute_ts<? ORDER BY m.minute_ts, m.series_id''', (start, end)) as cursor:
                while rows := cursor.fetchmany(128):
                    for record in rows:
                        row = dict(record)
                        row['archive_row_id'] = f"{row['series_id']}:{row['minute_ts']}"
                        payload = canonical_json(row)
                        copied_bytes += len(payload.encode())
                        if copied_bytes > max_bytes:
                            raise RuntimeError('archive byte budget exceeded; source retained')
                        dest.execute('INSERT INTO records VALUES (?, ?, ?, ?)', ('metric_minutes', row['archive_row_id'], count, payload))
                        digest.update(payload.encode()); digest.update(b'\n'); count += 1
            manifest['tables']['metric_minutes'] = {'count': count, 'sha256': digest.hexdigest(),
                'identity_column': 'archive_row_id', 'partition_clock': 'minute_ts (UTC minute)'}
            dest.execute('INSERT INTO manifest VALUES (?)', (canonical_json(manifest),))
            dest.commit()
        if staging.stat().st_size > max_bytes:
            raise RuntimeError('archive file exceeds byte budget; source retained')
        if verify_archive(staging) != manifest:
            raise RuntimeError('archive manifest mismatch')
        digest = hashlib.sha256(canonical_json(manifest).encode()).hexdigest()
        final = root / f'history-{day}-{digest}.sqlite3'
        staging.chmod(0o400)
        _sync_file(staging)
        try:
            os.link(staging, final)  # Atomic no-replace; never overwrite a completed file.
        except FileExistsError:
            if verify_archive(final) != manifest:
                raise RuntimeError('existing archive conflicts with source snapshot')
        _sync_dir(root)
        receipt = {'schema': 'monitoring_v4.archive_copy_receipt.v1', 'archive': final.name,
                   'manifest_sha256': digest, 'copy_only': True, 'source_deleted_rows': 0,
                   'rows': sum(t['count'] for t in manifest['tables'].values())}
        # If publication succeeded but receipt failed, rerun revalidates the
        # immutable file, then repairs the receipt. No source mutation is needed.
        atomic_write_text(root / (final.name + '.receipt.json'), canonical_json(receipt) + '\n', mode=0o600)
        return receipt
    finally:
        staging.unlink(missing_ok=True)
