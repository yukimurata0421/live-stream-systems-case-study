import importlib.util
import asyncio
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

SPEC = importlib.util.spec_from_file_location("player_av", Path(__file__).parents[1] / "ops/scripts/stream_v3_player_av_probe.py")
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def samples():
    return [dict(present=True, video_id="abcdefghijk", elapsed=i, media_time=i, frames=i*30,
                 audio_bytes=i*1000, rms=0.02, width=960, height=540, ready=4)
            for i in range(0, 76, 2)]


def test_av_requires_target_continuous_frames_and_audio():
    rows = samples()
    assert probe.assess(rows, "abcdefghijk")["status"] == "PASS"
    for field, value in [("frames", 1), ("audio_bytes", 0), ("rms", 0), ("video_id", "advert-id"),
                         ("ad", True), ("muted", True), ("ready", 0)]:
        changed = [dict(x, **{field: value}) for x in rows]
        assert probe.assess(changed, "abcdefghijk")["target_state"] == "UNKNOWN"


def test_error_or_reset_does_not_join_discontinuous_intervals():
    rows = samples()
    rows[20] = dict(rows[20], error=3)
    assert probe.assess(rows, "abcdefghijk")["status"] == "UNKNOWN"
    rows = samples()
    for x in rows[20:]:
        x["frames"] -= 1200
        x["media_time"] -= 40
    assert probe.assess(rows, "abcdefghijk")["status"] == "UNKNOWN"
    assert probe.assess([], "abcdefghijk")["status"] == "UNKNOWN"
    rows = samples()
    rows[-1] = dict(rows[-1], error=3)
    assert probe.assess(rows, "abcdefghijk")["status"] == "UNKNOWN"


def test_network_diagnostics_keep_error_codes_without_urls():
    assert probe.failure_code("net::ERR_ABORTED") == "net::ERR_ABORTED"
    assert probe.failure_code("net::ERR_FAILED https://example.invalid/?secret=x") == "unclassified"


def test_access_rejection_is_not_a_stream_failure_or_a_pass():
    report = {"status": "UNKNOWN", "network": [{"kind": "media", "status": 403}]}
    assert probe.diagnostics(report) == "media_access_rejected"
    assert report["status"] == "UNKNOWN"
    assert probe.diagnostics(dict(report, error="TimeoutError")) == "probe_execution_incomplete"
    assert probe.diagnostics(dict(report, status="PASS")) == "continuous_av_observed"
    # An unrelated failed document or image is not evidence of media rejection.
    assert probe.diagnostics({"network": [{"kind": "image", "status": 403}]}) == "continuous_av_unconfirmed"


def test_wrong_browser_digest_stops_before_playwright(tmp_path, monkeypatch):
    executable = tmp_path / "chrome"
    executable.write_bytes(b"unmatched browser")
    monkeypatch.setitem(sys.modules, "playwright.async_api", None)
    args = SimpleNamespace(chromium=executable, expected_browser_sha256="0" * 64,
                           expected_browser_version="157.0.8103.38", output=tmp_path / "result",
                           video_id="abcdefghijk", surface="embed", seconds=65,
                           verify_displayed_clock=False, headed=False)
    assert asyncio.run(probe.observe(args)) == 2
    report = json.loads((args.output / "player.json").read_text())
    assert report["diagnosis"] == "browser_sha256_mismatch"
    assert report["status"] == "UNKNOWN"
    assert report["samples"] == []


def test_wrong_running_version_closes_browser_without_opening_page(tmp_path, monkeypatch):
    executable = tmp_path / "chrome"
    executable.write_bytes(b"pinned browser")
    browser = SimpleNamespace(version="156.0.8078.4", new_page=AsyncMock(), close=AsyncMock())
    driver = AsyncMock()
    driver.__aenter__.return_value = SimpleNamespace(chromium=SimpleNamespace(
        launch=AsyncMock(return_value=browser)))
    monkeypatch.setitem(sys.modules, "playwright.async_api", SimpleNamespace(async_playwright=lambda: driver))
    args = SimpleNamespace(chromium=executable, expected_browser_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
                           expected_browser_version="157.0.8103.38", output=tmp_path / "result",
                           video_id="abcdefghijk", surface="embed", seconds=65,
                           verify_displayed_clock=False, headed=False, confined_no_sandbox=False)
    assert asyncio.run(probe.observe(args)) == 2
    browser.close.assert_awaited_once()
    browser.new_page.assert_not_called()
    report = json.loads((args.output / "player.json").read_text())
    assert report["diagnosis"] == "browser_version_mismatch"
    assert report["browser_version"] == "156.0.8078.4"


def test_matched_launcher_loads_local_profile_and_cli_overrides(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(probe.__file__).parent))
    import stream_v3_matched_player_probe as launcher

    monkeypatch.setattr(launcher.Path, "home", classmethod(lambda cls: tmp_path))
    config = tmp_path / ".config/stream-v3/matched-player.env"
    config.parent.mkdir(parents=True)
    config.write_text(
        "PLAYER_VIDEO_ID=abcdefghijk\n"
        f"PLAYER_ARTIFACT={tmp_path}/artifact\n"
        f"PLAYER_ARTIFACT_SHA256={'0' * 64}\n"
        f"PLAYER_PLAYWRIGHT_SITE={tmp_path}/playwright\n"
        f"PLAYER_PILLOW_SITE={tmp_path}/pillow\n"
        f"PLAYER_OCR_ROOT={tmp_path}/ocr\n"
        f"PLAYER_OUTPUT_ROOT={tmp_path}\n"
    )
    defaults = launcher.parse_args([])
    assert defaults.video_id == "abcdefghijk"
    assert defaults.artifact == tmp_path / "artifact"
    assert defaults.output_root == tmp_path
    assert defaults.output is None
    explicit = launcher.parse_args(["--video-id", "12345678901", "--output", str(tmp_path / "result")])
    assert explicit.video_id == "12345678901"
    assert explicit.output == tmp_path / "result"
    assert explicit.output_root is None


def test_matched_launcher_rejects_unrelated_settings_without_printing_values(tmp_path, monkeypatch, capsys):
    monkeypatch.syspath_prepend(str(Path(probe.__file__).parent))
    import stream_v3_matched_player_probe as launcher

    config = tmp_path / "mixed.env"
    config.write_text("UNRELATED_TOKEN=arbitrary-opaque-value\n")
    with pytest.raises(SystemExit) as error:
        launcher.parse_args(["--env-file", str(config)])
    assert error.value.code == 2
    stderr = capsys.readouterr().err
    assert "expected a unique PLAYER_* assignment" in stderr
    assert "arbitrary-opaque-value" not in stderr


def test_embed_has_an_actual_loopback_origin_and_only_generated_html():
    async def request():
        server, origin = await probe.embed_server("abcdefghijk")
        port = server.sockets[0].getsockname()[1]
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /etc/passwd HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            data = await reader.read()
            writer.close()
            await writer.wait_closed()
            assert origin.encode() in data
            assert b"https://www.youtube.com/embed/abcdefghijk?" in data
            assert b"root:" not in data
            assert b"Cache-Control: no-store" in data
        finally:
            server.close()
            await server.wait_closed()
        assert not server.is_serving()
    asyncio.run(request())


def test_embed_rejects_unbound_ids_and_origins():
    import pytest
    for video_id, origin in [("bad\"id", "http://127.0.0.1:123"),
                             ("abcdefghijk", "https://example.invalid")]:
        with pytest.raises(ValueError):
            probe.embed_html(video_id, origin)


def clock_rows():
    return [dict(elapsed=i, ocr_verified=True, clock_pair_consistent=True,
                 observer_timing_valid=True, display_age_seconds=30,
                 clock_state="WARMUP" if i == 0 else "ADVANCING_CONFIRMED")
            for i in range(0, 80, 10)]


def test_advancing_video_and_audio_do_not_make_old_or_unverified_content_current():
    av = probe.assess(samples(), "abcdefghijk")
    rows = clock_rows()
    assert probe.assess_displayed_clock(rows, av)["status"] == "PASS"
    for field, value in [("display_age_seconds", 3600), ("display_age_seconds", float("nan")),
                         ("display_age_seconds", -2), ("observer_timing_valid", False),
                         ("clock_pair_consistent", False), ("ocr_verified", False),
                         ("clock_state", "STALLED_SUSPECT"), ("clock_state", "WARMUP")]:
        changed = [dict(r, **{field: value}) for r in rows]
        assert probe.assess_displayed_clock(changed, av)["status"] == "UNKNOWN"
    assert av["status"] == "PASS"  # Clock uncertainty is not a media failure.


def test_clock_and_av_proof_must_cover_the_same_continuous_window():
    av = probe.assess(samples(), "abcdefghijk")
    rows = clock_rows()
    rows[3] = dict(rows[3], ocr_verified=False)
    assert probe.assess_displayed_clock(rows, av)["status"] == "UNKNOWN"
    assert probe.assess_displayed_clock(clock_rows()[:3], av)["status"] == "UNKNOWN"
    disjoint = dict(av, interval_start_elapsed=60, interval_end_elapsed=130)
    assert probe.assess_displayed_clock(clock_rows(), disjoint)["status"] == "UNKNOWN"


def test_reusing_a_clock_reading_keeps_increasing_its_original_age():
    av = probe.assess(samples(), "abcdefghijk")
    rows = [dict(r, display_age_seconds=58) for r in clock_rows()]
    result = probe.assess_displayed_clock(rows, av)
    assert result["current_age_upper_seconds"] == 63
    assert result["status"] == "UNKNOWN"


def test_committed_profile_cold_loads_its_real_clock_dependencies(tmp_path):
    import json
    import shutil
    import subprocess
    import sys

    root = Path(__file__).parents[1]
    profile_path = "configs/releases/public-player-probe.json"
    profile = json.loads((root / profile_path).read_text())
    source = tmp_path / "source"
    source.mkdir()
    for name in [profile_path, *[row["source"] for row in profile["files"]]]:
        destination = source / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / name, destination)
    def git(*args):
        return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()
    git("init", "-q")
    git("add", ".")
    git("-c", "user.name=yukimurata0421", "-c", "user.email=sss071137@gmail.com",
        "commit", "-qm", "Freeze player fixture")
    artifact = tmp_path / "artifact"
    subprocess.run([sys.executable, str(root / "ops/scripts/stream_v3_release.py"), "build",
                    "--repo", str(source), "--commit", git("rev-parse", "HEAD"),
                    "--profile", profile_path, "--output", str(artifact)],
                   check=True, capture_output=True, timeout=20)
    entry = artifact / "ops/scripts/stream_v3_player_av_probe.py"
    script = """import runpy, sys
from types import SimpleNamespace
from pathlib import Path
module = runpy.run_path(sys.argv[1], run_name='cold_profile')
reader = module['DisplayedClock'](SimpleNamespace(tesseract='unused', tessdata_dir=None))
assert reader.clock.capture()['mono'] >= 0
assert Path(reader.clock.__file__).is_relative_to(Path(sys.argv[1]).parents[2])
print(getattr(reader.clock, '_source', reader.clock.__file__))
"""
    command = [sys.executable, "-I", "-B", "-c", script, str(entry)]
    result = subprocess.run(command, check=True, capture_output=True, timeout=10)
    # Resolve the required file through the real adapter. The portable copy and
    # host-bound adapter can use different layouts; both must be self-contained.
    required = Path(result.stdout.decode().strip())
    assert required.is_relative_to(artifact)
    required.unlink()
    assert subprocess.run(command, capture_output=True, timeout=10).returncode != 0
