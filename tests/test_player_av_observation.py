import importlib.util
from pathlib import Path

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
