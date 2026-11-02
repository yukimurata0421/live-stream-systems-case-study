import sqlite3
from stream_core.operational_reliability import evidence_store


def test_report_reads_uncheckpointed_wal_without_initializing_or_mutating(tmp_path,monkeypatch):
    path=tmp_path/'ledger.sqlite'
    db=sqlite3.connect(path);db.execute('PRAGMA journal_mode=WAL')
    db.execute('CREATE TABLE same_url_transition_ledger (observed_ts INTEGER,observed_day_jst TEXT,event_type TEXT,controlled INTEGER,live_url_sha256 TEXT,previous_live_url_sha256 TEXT)')
    db.execute('INSERT INTO same_url_transition_ledger VALUES (100,"day","url_transition",0,"hash","prior")');db.commit()
    before=path.read_bytes();wal=path.with_name(path.name+'-wal').read_bytes()
    monkeypatch.setattr(evidence_store,'connect',lambda *_: (_ for _ in ()).throw(AssertionError('writer called')))
    result=evidence_store.same_url_ledger_evidence(path,start_ts=0,end_ts=200)
    assert result['row_count']==1 and result['uncontrolled_transition_count']==1
    assert path.read_bytes()==before and path.with_name(path.name+'-wal').read_bytes()==wal
    db.close()


def test_missing_report_database_is_not_created(tmp_path):
    path=tmp_path/'missing'/'ledger.sqlite'
    assert evidence_store.same_url_ledger_evidence(path,start_ts=0,end_ts=200)=={'available':False,'row_count':0}
    assert not path.parent.exists()


def test_checkpointed_wal_database_needs_no_writable_sidecars(tmp_path):
    path=tmp_path/'ledger.sqlite';db=sqlite3.connect(path);db.execute('PRAGMA journal_mode=WAL')
    db.execute('CREATE TABLE same_url_transition_ledger (observed_ts INTEGER,observed_day_jst TEXT,event_type TEXT,controlled INTEGER,live_url_sha256 TEXT,previous_live_url_sha256 TEXT)')
    db.commit();db.close()
    assert not path.with_name(path.name+'-wal').exists()
    before=path.stat()
    assert evidence_store.same_url_ledger_evidence(path,start_ts=0,end_ts=200)['row_count']==0
    assert path.stat().st_mtime_ns==before.st_mtime_ns
    assert not path.with_name(path.name+'-wal').exists()
    assert not path.with_name(path.name+'-shm').exists()


def test_writer_appearing_during_checkpointed_read_is_rejected(tmp_path,monkeypatch):
    import pytest
    path=tmp_path/'ledger.sqlite';db=sqlite3.connect(path)
    db.execute('CREATE TABLE same_url_transition_ledger (observed_ts INTEGER,observed_day_jst TEXT,event_type TEXT,controlled INTEGER,live_url_sha256 TEXT,previous_live_url_sha256 TEXT)');db.commit();db.close()
    original=sqlite3.connect
    def connect(database,*args,**kwargs):
        result=original(database,*args,**kwargs)
        path.with_name(path.name+'-wal').write_bytes(b'writer started')
        return result
    monkeypatch.setattr(sqlite3,'connect',connect)
    with pytest.raises(sqlite3.OperationalError,match='ledger_changed_during_read'):
        evidence_store.same_url_ledger_evidence(path,start_ts=0,end_ts=200)
