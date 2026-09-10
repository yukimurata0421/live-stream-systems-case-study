from __future__ import annotations

import importlib.util
import json
import subprocess
import time
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest

RECOVERY_CONTROL_ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_CONTAINER = RECOVERY_CONTROL_ROOT.parent
STREAM_V3_ROOT = REPOSITORY_CONTAINER if (REPOSITORY_CONTAINER / "src" / "stream_v3").is_dir() else REPOSITORY_CONTAINER / "stream_v3"
SCRIPT = STREAM_V3_ROOT / "ops/public-publisher/site/scripts/push_to_gcs.py"


@pytest.fixture
def publisher(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    spec = importlib.util.spec_from_file_location("publication_hardening_candidate", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "SITE_DIR", tmp_path)
    monkeypatch.setattr(module, "PUBLIC_DIR", tmp_path / "public")
    monkeypatch.setattr(module, "DEST", "gs://test-public-bucket/site")
    monkeypatch.setattr(module, "ensure_gcloud", lambda: None)
    return module


def populate(module: ModuleType, *, generated: float | None = None) -> None:
    module.PUBLIC_DIR.mkdir()
    (module.PUBLIC_DIR / "index.html").write_text("public index")
    for name in module.REQUIRED_JSON:
        (module.PUBLIC_DIR / name).write_text(json.dumps({"generated_at": generated if generated is not None else time.time()}))
    (module.PUBLIC_DIR / "assets").mkdir()
    (module.PUBLIC_DIR / "assets/site.js").write_text("// public")
    (module.PUBLIC_DIR / "assets/site.js.bak.old").write_text("not for publication")
    (module.PUBLIC_DIR / "private.db").write_bytes(b"not for publication")


def test_allowlist_never_publishes_backups_or_database(publisher: ModuleType, tmp_path: Path) -> None:
    populate(publisher)
    hashes = publisher.stage_public_tree(publisher.PUBLIC_DIR, tmp_path / "staged", now=time.time())
    assert "assets/site.js" in hashes
    assert all(".bak" not in name and ".db" not in name for name in hashes)


@pytest.mark.parametrize("generated", [0, float("nan"), float("inf"), True, "yesterday"])
def test_bad_or_stale_source_never_uploads(publisher: ModuleType, tmp_path: Path, generated: object) -> None:
    populate(publisher)
    (publisher.PUBLIC_DIR / publisher.REQUIRED_JSON[0]).write_text(json.dumps({"generated_at": generated}))
    with pytest.raises(ValueError):
        publisher.stage_public_tree(publisher.PUBLIC_DIR, tmp_path / "staged", now=time.time())


def test_future_source_is_not_fresh(publisher: ModuleType, tmp_path: Path) -> None:
    populate(publisher, generated=time.time() + 60)
    with pytest.raises(ValueError, match="FUTURE"):
        publisher.stage_public_tree(publisher.PUBLIC_DIR, tmp_path / "staged", now=time.time())


def test_symlink_cannot_export_other_files(publisher: ModuleType, tmp_path: Path) -> None:
    populate(publisher)
    (publisher.PUBLIC_DIR / "index.html").unlink()
    secret = tmp_path / "private-source"
    secret.write_text("private")
    (publisher.PUBLIC_DIR / "index.html").symlink_to(secret)
    with pytest.raises((OSError, ValueError)):
        publisher.stage_public_tree(publisher.PUBLIC_DIR, tmp_path / "staged", now=time.time())


def test_duplicate_json_key_is_rejected(publisher: ModuleType, tmp_path: Path) -> None:
    populate(publisher)
    (publisher.PUBLIC_DIR / publisher.REQUIRED_JSON[0]).write_text('{"generated_at":0,"generated_at":1}')
    with pytest.raises(ValueError, match="DUPLICATE"):
        publisher.stage_public_tree(publisher.PUBLIC_DIR, tmp_path / "staged", now=time.time())


@pytest.mark.parametrize("destination", ["", "gs://", "gs://bucket", "gs://bucket/", "gs://bucket/../other", "gs://bucket/*"])
def test_destination_must_be_explicit_scoped_prefix(publisher: ModuleType, monkeypatch: pytest.MonkeyPatch, destination: str) -> None:
    monkeypatch.setattr(publisher, "DEST", destination)
    with pytest.raises(RuntimeError):
        publisher.ensure_destination()


def test_main_records_upload_not_external_visibility_and_preserves_failures(
    publisher: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    populate(publisher)
    calls = []
    monkeypatch.setattr(publisher, "run", lambda args, **kwargs: calls.append((args, kwargs)))
    assert publisher.main() == 0
    state = json.loads((publisher.SITE_DIR / ".state/public-push.json").read_text())
    assert state["status"] == "SUCCEEDED"
    assert state["public_mirror_verified"] is False
    assert len(calls) == 3
    assert calls[0][1]["deadline"] == calls[2][1]["deadline"]
    assert not any("objects" in args or "--delete-unmatched-destination-objects" in args for args, _ in calls)
    assert "site.js.bak.old" not in json.dumps(state)


def test_failed_upload_is_durable_and_not_erased_by_success(publisher: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    populate(publisher)

    def fail(*args: object, **kwargs: object) -> None:
        raise TimeoutError("PUBLICATION_DEADLINE_EXCEEDED")

    monkeypatch.setattr(publisher, "run", fail)
    with pytest.raises(TimeoutError):
        publisher.main()
    path = publisher.SITE_DIR / ".state/public-push.json"
    assert json.loads(path.read_text())["failure_count"] == 1
    monkeypatch.setattr(publisher, "run", lambda *args, **kwargs: None)
    publisher.main()
    result = json.loads(path.read_text())
    assert result["status"] == "SUCCEEDED" and result["failure_count"] == 1
    assert result["last_failure_at"] is not None


def test_deadline_kills_only_owned_process_group(publisher: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    process = MagicMock()
    process.pid = 12345
    process.__enter__.return_value = process
    process.communicate.side_effect = [subprocess.TimeoutExpired("owned-fixture", 1), (None, None)]
    monkeypatch.setattr(publisher.subprocess, "Popen", lambda *args, **kwargs: process)
    kill = MagicMock()
    monkeypatch.setattr(publisher.os, "killpg", kill)
    with pytest.raises(TimeoutError):
        publisher.run(["owned-fixture"], deadline=time.monotonic() + 1)
    kill.assert_called_once_with(12345, publisher.signal.SIGKILL)


def test_status_cannot_be_written_to_public_tree(publisher: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    populate(publisher)
    monkeypatch.setenv("YUKIMURATA_PUBLISH_STATUS_FILE", str(publisher.PUBLIC_DIR / "internal-state.json"))
    with pytest.raises(ValueError, match="PRIVATE"):
        publisher.main()


def test_publisher_state_cannot_follow_links_or_reset_negative_counter(publisher: ModuleType, tmp_path: Path) -> None:
    state = tmp_path / "state.json"
    state.write_text('{"failure_count":-1}')
    with pytest.raises(ValueError, match="COUNTER"):
        publisher.read_status(state)
    link = tmp_path / "state-link.json"
    link.symlink_to(state)
    with pytest.raises(OSError):
        publisher.read_status(link)
