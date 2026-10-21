"""The public collector must reject missing machine and root-owner bindings."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import stat
import pytest

spec=importlib.util.spec_from_file_location('backup_collector',Path(__file__).parents[1]/'ops/oci-backup/collect.py')
collector=importlib.util.module_from_spec(spec);spec.loader.exec_module(collector)


@pytest.mark.parametrize('uid,mode,host,machine,accepted',[
    (0,stat.S_IFREG|0o600,'relay-host','fixture-machine',True),
    (1,stat.S_IFREG|0o600,'relay-host','fixture-machine',False),
    (0,stat.S_IFREG|0o622,'relay-host','fixture-machine',False),
    (0,stat.S_IFLNK|0o600,'relay-host','fixture-machine',False),
    (0,stat.S_IFREG|0o600,'other','fixture-machine',False),
    (0,stat.S_IFREG|0o600,'relay-host','different-machine',False),
])
def test_source_identity_remains_bound(monkeypatch,uid,mode,host,machine,accepted):
    class Config:
        def lstat(self):return SimpleNamespace(st_uid=uid,st_mode=mode)
        def read_text(self):return json.dumps({'host_id':host,'machine_id_sha256':machine})
    monkeypatch.setattr(collector,'digest',lambda _: 'fixture-machine')
    if accepted:assert collector.source_identity(Config())['host_id']=='relay-host'
    else:
        with pytest.raises(ValueError):collector.source_identity(Config())
