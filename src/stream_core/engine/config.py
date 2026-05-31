from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


SUPPORTED_VIDEO_ENCODERS = {"libx264", "h264_nvenc"}


def to_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def to_int(value: str, default: int) -> int:
    try:
        return int(value.strip())
    except Exception:
        return default


def to_float(value: str, default: float) -> float:
    try:
        return float(value.strip())
    except Exception:
        return default


@dataclass
class Config:
    base_dir: Path
    now_playing_file: Path
    rtmp_url: str
    stream_key: str
    test_mode: bool
    test_output: str
    test_output_file: Path
    display_name: str
    video_size: str
    frame_rate: int
    output_size: str
    draw_mouse: int
    display_input: str
    display_offset: str
    auto_start_xvfb: bool
    auto_start_browser: bool
    browser_url: str
    browser_bin: str
    use_overlay_wrapper: bool
    overlay_dir: Path
    overlay_bind_host: str
    overlay_view_host: str
    overlay_port: int
    overlay_server_log_file: Path
    stream1090_url: str
    map_lat: str
    map_lon: str
    map_zoom: str
    map_scale: str
    map_icon_scale: str
    map_label_scale: str
    map_large_mode: str
    browser_profile_dir: Path
    reset_browser_profile: bool
    browser_window_size: str
    browser_window_pos: str
    browser_start_settle_sec: float
    browser_start_settle_sec_restart: float
    browser_start_settle_sec_test: float
    xvfb_depth: int
    xvfb_log_file: Path
    browser_log_file: Path
    pulse_sink: str
    pulse_source: str
    pulse_shm: str
    local_monitor_audio: bool
    monitor_sink: str
    monitor_loopback_latency_msec: int
    font_file: str
    restart_delay_sec: int
    stream_lock_dir: Path
    require_systemd_launch: bool
    allow_direct_stream_sh: bool
    health_gate_abort_on_foreign: bool
    runtime_state_file: Path
    runtime_heartbeat_sec: int
    stop_ffmpeg_term_grace_sec: float
    capture_helper_memory_guard_enabled: bool
    xvfb_memory_guard_rss_mib: int
    xvfb_memory_guard_shmem_mib: int
    takeover_enabled: bool
    takeover_grace_sec: int
    takeover_force_kill: bool
    video_encoder: str
    video_preset: str
    video_nvenc_preset: str
    video_nvenc_rc: str
    video_nvenc_cq: str
    video_nvenc_multipass: str
    video_nvenc_rc_lookahead: int
    video_nvenc_spatial_aq: bool
    video_nvenc_temporal_aq: bool
    video_nvenc_bframes: int
    video_nvenc_b_ref_mode: str
    video_bitrate: str
    video_maxrate: str
    video_bufsize: str
    emergency_low_upload_enabled: bool
    emergency_low_upload_triggers: tuple[str, ...]
    emergency_low_upload_duration_sec: int
    emergency_low_upload_video_bitrate: str
    emergency_low_upload_video_maxrate: str
    emergency_low_upload_video_bufsize: str
    emergency_low_upload_audio_bitrate: str
    audio_bitrate: str
    audio_sample_rate: int
    audio_queue_size: int
    audio_filter: str
    use_fifo_recovery: bool
    fifo_queue_size: int
    fifo_recovery_wait_sec: int
    fifo_max_recovery_attempts: int
    fifo_drop_pkts_on_overflow: bool
    fifo_restart_with_keyframe: bool
    event_log_file: Path
    restart_reason_file: Path
    pre_ffmpeg_min_wait_sec: float
    pre_ffmpeg_min_wait_sec_restart: float
    pre_ffmpeg_min_wait_sec_test: float
    pre_ffmpeg_restart_context_max_age_sec: int
    pre_ffmpeg_overlay_ready_timeout_sec: float
    pre_ffmpeg_overlay_ready_poll_sec: float
    pre_ffmpeg_require_overlay_ready: bool
    script_dir: Path


def normalize_video_encoder(raw: str) -> str:
    value = raw.strip().lower()
    if value not in SUPPORTED_VIDEO_ENCODERS:
        supported = ", ".join(sorted(SUPPORTED_VIDEO_ENCODERS))
        raise ValueError(f"Unsupported VIDEO_ENCODER={raw!r}. Supported values: {supported}")
    return value


def load_config() -> Config:
    script_dir = Path(__file__).resolve().parents[1]
    base_default = script_dir.parent.parent
    base_dir = Path(os.environ.get("BASE_DIR", str(base_default))).resolve()

    def e(name: str, default: str) -> str:
        return os.environ.get(name, default)

    display_name = e("DISPLAY_NAME", os.environ.get("DISPLAY", ":99"))
    return Config(
        base_dir=base_dir,
        now_playing_file=Path(e("NOW_PLAYING_FILE", str(base_dir / "now_playing.txt"))),
        rtmp_url=e("RTMP_URL", "rtmps://a.rtmps.youtube.com:443/live2/YOUR_STREAM_KEY"),
        stream_key=e("STREAM_KEY", ""),
        test_mode=to_bool(e("TEST_MODE", "0")),
        test_output=e("TEST_OUTPUT", "null"),
        test_output_file=Path(e("TEST_OUTPUT_FILE", str(base_dir / "test_capture.mkv"))),
        display_name=display_name,
        video_size=e("VIDEO_SIZE", "1920x1080"),
        frame_rate=max(1, to_int(e("FRAME_RATE", "5"), 5)),
        output_size=e("OUTPUT_SIZE", "1920x1080"),
        draw_mouse=to_int(e("DRAW_MOUSE", "0"), 0),
        display_input=e("DISPLAY_INPUT", ""),
        display_offset=e("DISPLAY_OFFSET", "+0,0"),
        auto_start_xvfb=to_bool(e("AUTO_START_XVFB", "1"), True),
        auto_start_browser=to_bool(e("AUTO_START_BROWSER", "1"), True),
        browser_url=e("BROWSER_URL", "http://stream1090.lan/stream1090/"),
        browser_bin=e("BROWSER_BIN", ""),
        use_overlay_wrapper=to_bool(e("USE_OVERLAY_WRAPPER", "1"), True),
        overlay_dir=Path(e("OVERLAY_DIR", str(base_dir / "ui" / "overlay"))),
        overlay_bind_host=e("OVERLAY_BIND_HOST", "0.0.0.0"),
        overlay_view_host=e("OVERLAY_VIEW_HOST", "127.0.0.1"),
        overlay_port=max(1, to_int(e("OVERLAY_PORT", "18080"), 18080)),
        overlay_server_log_file=Path(e("OVERLAY_SERVER_LOG_FILE", str(base_dir / "logs" / "overlay_server.log"))),
        stream1090_url=e("STREAM1090_URL", "http://stream1090.lan/stream1090/"),
        map_lat=e("MAP_LAT", "36.35"),
        map_lon=e("MAP_LON", "140.75"),
        map_zoom=e("MAP_ZOOM", "7.6"),
        map_scale=e("MAP_SCALE", "0.82"),
        map_icon_scale=e("MAP_ICON_SCALE", "1.4"),
        map_label_scale=e("MAP_LABEL_SCALE", "0.82"),
        map_large_mode=e("MAP_LARGE_MODE", "1"),
        browser_profile_dir=Path(e("BROWSER_PROFILE_DIR", str(base_dir / "runtime" / "chromium_profile"))),
        reset_browser_profile=to_bool(e("RESET_BROWSER_PROFILE", "1"), True),
        browser_window_size=e("BROWSER_WINDOW_SIZE", ""),
        browser_window_pos=e("BROWSER_WINDOW_POS", "0,0"),
        browser_start_settle_sec=max(0.0, to_float(e("BROWSER_START_SETTLE_SEC", "2"), 2.0)),
        browser_start_settle_sec_restart=max(0.0, to_float(e("BROWSER_START_SETTLE_SEC_RESTART", "0.5"), 0.5)),
        browser_start_settle_sec_test=max(0.0, to_float(e("BROWSER_START_SETTLE_SEC_TEST", "0"), 0.0)),
        xvfb_depth=max(16, to_int(e("XVFB_DEPTH", "24"), 24)),
        xvfb_log_file=Path(e("XVFB_LOG_FILE", str(base_dir / "logs" / "xvfb.log"))),
        browser_log_file=Path(e("BROWSER_LOG_FILE", str(base_dir / "logs" / "browser.log"))),
        pulse_sink=e("PULSE_SINK", "stream_sink"),
        pulse_source=e("PULSE_SOURCE", ""),
        pulse_shm=e("PULSE_SHM", "0"),
        local_monitor_audio=to_bool(e("LOCAL_MONITOR_AUDIO", "0")),
        monitor_sink=e("MONITOR_SINK", ""),
        monitor_loopback_latency_msec=max(10, to_int(e("MONITOR_LOOPBACK_LATENCY_MSEC", "60"), 60)),
        font_file=e("FONT_FILE", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
        restart_delay_sec=max(1, to_int(e("RESTART_DELAY_SEC", "5"), 5)),
        stream_lock_dir=Path(e("STREAM_LOCK_DIR", "/var/tmp")),
        require_systemd_launch=to_bool(e("REQUIRE_SYSTEMD_LAUNCH", "1"), True),
        allow_direct_stream_sh=to_bool(e("ALLOW_DIRECT_STREAM_SH", "0"), False),
        health_gate_abort_on_foreign=to_bool(e("HEALTH_GATE_ABORT_ON_FOREIGN", "1"), True),
        runtime_state_file=Path(e("RUNTIME_STATE_FILE", str(base_dir / "state" / "runtime" / "stream_runtime_state.json"))),
        runtime_heartbeat_sec=max(5, to_int(e("RUNTIME_HEARTBEAT_SEC", "30"), 30)),
        stop_ffmpeg_term_grace_sec=max(0.5, to_float(e("STOP_FFMPEG_TERM_GRACE_SEC", "3"), 3.0)),
        capture_helper_memory_guard_enabled=to_bool(e("CAPTURE_HELPER_MEMORY_GUARD_ENABLED", "1"), True),
        xvfb_memory_guard_rss_mib=max(0, to_int(e("XVFB_MEMORY_GUARD_RSS_MIB", "2048"), 2048)),
        xvfb_memory_guard_shmem_mib=max(0, to_int(e("XVFB_MEMORY_GUARD_SHMEM_MIB", "1536"), 1536)),
        takeover_enabled=to_bool(e("TAKEOVER_ENABLED", "1"), True),
        takeover_grace_sec=max(1, to_int(e("TAKEOVER_GRACE_SEC", "5"), 5)),
        takeover_force_kill=to_bool(e("TAKEOVER_FORCE_KILL", "1"), True),
        video_encoder=normalize_video_encoder(e("VIDEO_ENCODER", "libx264")),
        video_preset=e("VIDEO_PRESET", "ultrafast"),
        video_nvenc_preset=e("VIDEO_NVENC_PRESET", "p4"),
        video_nvenc_rc=e("VIDEO_NVENC_RC", "cbr"),
        video_nvenc_cq=e("VIDEO_NVENC_CQ", ""),
        video_nvenc_multipass=e("VIDEO_NVENC_MULTIPASS", ""),
        video_nvenc_rc_lookahead=max(0, to_int(e("VIDEO_NVENC_RC_LOOKAHEAD", "0"), 0)),
        video_nvenc_spatial_aq=to_bool(e("VIDEO_NVENC_SPATIAL_AQ", "0")),
        video_nvenc_temporal_aq=to_bool(e("VIDEO_NVENC_TEMPORAL_AQ", "0")),
        video_nvenc_bframes=max(0, to_int(e("VIDEO_NVENC_BFRAMES", "0"), 0)),
        video_nvenc_b_ref_mode=e("VIDEO_NVENC_B_REF_MODE", ""),
        video_bitrate=e("VIDEO_BITRATE", "3400k"),
        video_maxrate=e("VIDEO_MAXRATE", "3400k"),
        video_bufsize=e("VIDEO_BUFSIZE", "6800k"),
        emergency_low_upload_enabled=to_bool(e("EMERGENCY_LOW_UPLOAD_ENABLED", "1"), True),
        emergency_low_upload_triggers=tuple(
            item.strip()
            for item in e("EMERGENCY_LOW_UPLOAD_TRIGGERS", "network_down,low_upload_pressure").split(",")
            if item.strip()
        ),
        emergency_low_upload_duration_sec=max(60, to_int(e("EMERGENCY_LOW_UPLOAD_DURATION_SEC", "900"), 900)),
        emergency_low_upload_video_bitrate=e("EMERGENCY_LOW_UPLOAD_VIDEO_BITRATE", "2500k"),
        emergency_low_upload_video_maxrate=e("EMERGENCY_LOW_UPLOAD_VIDEO_MAXRATE", "2500k"),
        emergency_low_upload_video_bufsize=e("EMERGENCY_LOW_UPLOAD_VIDEO_BUFSIZE", "5000k"),
        emergency_low_upload_audio_bitrate=e("EMERGENCY_LOW_UPLOAD_AUDIO_BITRATE", ""),
        audio_bitrate=e("AUDIO_BITRATE", "192k"),
        audio_sample_rate=max(8000, to_int(e("AUDIO_SAMPLE_RATE", "48000"), 48000)),
        audio_queue_size=max(256, to_int(e("AUDIO_QUEUE_SIZE", "8192"), 8192)),
        audio_filter=e("AUDIO_FILTER", "aresample=async=1:min_hard_comp=0.030000:first_pts=0,volume=0.25"),
        use_fifo_recovery=to_bool(e("RTMP_FIFO_RECOVERY", "0"), False),
        fifo_queue_size=max(32, to_int(e("RTMP_FIFO_QUEUE_SIZE", "600"), 600)),
        fifo_recovery_wait_sec=max(1, to_int(e("RTMP_FIFO_RECOVERY_WAIT_SEC", "1"), 1)),
        fifo_max_recovery_attempts=max(0, to_int(e("RTMP_FIFO_MAX_RECOVERY_ATTEMPTS", "0"), 0)),
        fifo_drop_pkts_on_overflow=to_bool(e("RTMP_FIFO_DROP_PKTS_ON_OVERFLOW", "0")),
        fifo_restart_with_keyframe=to_bool(e("RTMP_FIFO_RESTART_WITH_KEYFRAME", "1"), True),
        event_log_file=Path(e("EVENT_LOG_FILE", str(base_dir / "logs" / "stream_engine_events.jsonl"))),
        restart_reason_file=Path(e("RESTART_REASON_FILE", str(base_dir / "state" / "runtime" / "restart_reason.json"))),
        pre_ffmpeg_min_wait_sec=max(0.0, to_float(e("PRE_FFMPEG_MIN_WAIT_SEC", "0"), 0.0)),
        pre_ffmpeg_min_wait_sec_restart=max(0.0, to_float(e("PRE_FFMPEG_MIN_WAIT_SEC_RESTART", "0"), 0.0)),
        pre_ffmpeg_min_wait_sec_test=max(0.0, to_float(e("PRE_FFMPEG_MIN_WAIT_SEC_TEST", "0"), 0.0)),
        pre_ffmpeg_restart_context_max_age_sec=max(0, to_int(e("PRE_FFMPEG_RESTART_CONTEXT_MAX_AGE_SEC", "300"), 300)),
        pre_ffmpeg_overlay_ready_timeout_sec=max(1.0, to_float(e("PRE_FFMPEG_OVERLAY_READY_TIMEOUT_SEC", "20"), 20.0)),
        pre_ffmpeg_overlay_ready_poll_sec=max(0.2, to_float(e("PRE_FFMPEG_OVERLAY_READY_POLL_SEC", "1"), 1.0)),
        pre_ffmpeg_require_overlay_ready=to_bool(e("PRE_FFMPEG_REQUIRE_OVERLAY_READY", "0")),
        script_dir=script_dir,
    )
