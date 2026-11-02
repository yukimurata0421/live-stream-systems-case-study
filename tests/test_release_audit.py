import importlib.util
import json
from pathlib import Path
import os
import select
import subprocess
import sys
from urllib.request import urlopen

import pytest

SPEC = importlib.util.spec_from_file_location("release_audit", Path(__file__).parents[1] / "ops/scripts/stream_v3_release_audit.py")
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def registry(tmp_path):
    source = tmp_path / "producer"
    source.mkdir()
    (source / "entry.py").write_text("original")
    d = {"schema": "stream.release_registry.v1", "commit": "a"*40,
         "components": [{"name": "producer", "root": str(source), "kind": "legacy-source-pinned",
                         "files": {"entry.py": module.sha(b"original")}}]}
    p = tmp_path / "registry.json"
    p.write_text(json.dumps(d))
    return p, source


def test_source_drift_is_not_health_or_loaded_memory_proof(tmp_path):
    p, source = registry(tmp_path)
    d = module.load(p)
    row, = module.audit(d)
    assert row["available"] and row["source_match"]
    assert row["configured_entrypoint_match"] is None
    (source / "entry.py").write_text("modified")
    row, = module.audit(d)
    assert row["available"] and not row["source_match"]
    (source / "entry.py").unlink()
    row, = module.audit(d)
    assert not row["available"] and not row["source_match"]
    assert 'kind="legacy-source-pinned"' in module.render(d, [row])


def test_audit_does_not_follow_source_symlinks(tmp_path):
    p, source = registry(tmp_path)
    d = module.load(p)
    (source / "entry.py").unlink()
    other = tmp_path / "outside"
    other.write_text("original")
    (source / "entry.py").symlink_to(other)
    assert not module.audit(d)[0]["available"]


def test_pointer_drift_does_not_report_the_unused_old_source_as_current(tmp_path):
    p, source = registry(tmp_path)
    pointer = tmp_path / "current"
    pointer.symlink_to(source)
    d = module.load(p)
    d["components"][0]["pointer"] = str(pointer)
    assert module.audit(d)[0]["source_match"]
    pointer.unlink()
    other = tmp_path / "different-release"
    other.mkdir()
    pointer.symlink_to(other)
    assert not module.audit(d)[0]["source_match"]


def test_registry_rejects_paths_and_caches_only_bounded_period(tmp_path):
    p, source = registry(tmp_path)
    reader = module.CachedAudit(p)
    assert "source_match{component=\"producer\"} 1" in reader.read()
    (source / "entry.py").write_text("modified")
    reader.checked -= 61
    assert "source_match{component=\"producer\"} 0" in reader.read()
    d = json.loads(p.read_text())
    d["components"][0]["files"] = {"../escape": "a"*64}
    p.write_text(json.dumps(d))
    with pytest.raises(ValueError):
        module.load(p)


@pytest.mark.parametrize("case", ["matching", "drift", "missing"])
def test_real_adapter_http_reports_source_provenance_separately(tmp_path, case):
    p, source = registry(tmp_path)
    if case == "drift":
        (source / "entry.py").write_text("modified")
    elif case == "missing":
        (source / "entry.py").unlink()
    script = Path(__file__).parents[1] / "ops/scripts/stream_v3_monitor_observation_exporter.py"
    child = subprocess.Popen([sys.executable, "-I", "-B", str(script), "--state-root", str(tmp_path / "state"),
        "--clock-mesh", str(tmp_path / "no-mesh"), "--source-host", "test-observer", "--port", "0",
        "--release-registry", str(p)], cwd=tmp_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, env={"PATH": os.environ.get("PATH", "/usr/bin"), "PYTHONDONTWRITEBYTECODE": "1"})
    try:
        ready, _, _ = select.select([child.stdout], [], [], 5)
        assert ready
        line = child.stdout.readline()
        assert line, child.stderr.read() if child.poll() is not None else "no startup report"
        port = json.loads(line)["port"]
        with urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
            text = response.read(128 * 1024).decode()
        assert 'stream_v3_observed_release_info{component="producer"' in text
        expected = int(case == "matching")
        assert f'stream_v3_observed_release_source_match{{component="producer"}} {expected}' in text
        assert 'state="UNKNOWN"' in text
    finally:
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
