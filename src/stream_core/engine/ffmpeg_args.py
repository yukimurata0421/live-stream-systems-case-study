from __future__ import annotations

from typing import Any


def build_output_args(cfg: Any) -> list[str]:
    if cfg.test_mode:
        if cfg.test_output == "file":
            return ["-f", "matroska", str(cfg.test_output_file)]
        return ["-f", "null", "-"]
    if cfg.use_fifo_recovery:
        return [
            "-f",
            "fifo",
            "-fifo_format",
            "flv",
            "-attempt_recovery",
            "1",
            "-recover_any_error",
            "1",
            "-recovery_wait_time",
            str(cfg.fifo_recovery_wait_sec),
            "-max_recovery_attempts",
            str(cfg.fifo_max_recovery_attempts),
            "-queue_size",
            str(cfg.fifo_queue_size),
            "-drop_pkts_on_overflow",
            "1" if cfg.fifo_drop_pkts_on_overflow else "0",
            "-restart_with_keyframe",
            "1" if cfg.fifo_restart_with_keyframe else "0",
            cfg.rtmp_url,
        ]
    return ["-f", "flv", cfg.rtmp_url]


def build_filter(output_size: str) -> str:
    width, height = output_size.split("x", 1)
    return f"scale={width}:{height}:flags=lanczos,setsar=1"


def build_video_encoder_args(cfg: Any) -> list[str]:
    encoder = str(getattr(cfg, "video_encoder", "libx264") or "libx264").strip().lower()
    if encoder == "libx264":
        return ["-c:v", "libx264", "-preset", cfg.video_preset]
    if encoder == "h264_nvenc":
        args = [
            "-c:v",
            "h264_nvenc",
            "-preset",
            str(getattr(cfg, "video_nvenc_preset", "p4") or "p4"),
            "-rc",
            str(getattr(cfg, "video_nvenc_rc", "cbr") or "cbr"),
        ]
        cq = str(getattr(cfg, "video_nvenc_cq", "") or "").strip()
        if cq:
            args.extend(["-cq", cq])
        multipass = str(getattr(cfg, "video_nvenc_multipass", "") or "").strip()
        if multipass:
            args.extend(["-multipass", multipass])
        lookahead = int(getattr(cfg, "video_nvenc_rc_lookahead", 0) or 0)
        if lookahead > 0:
            args.extend(["-rc-lookahead", str(lookahead)])
        if bool(getattr(cfg, "video_nvenc_spatial_aq", False)):
            args.extend(["-spatial-aq", "1"])
        if bool(getattr(cfg, "video_nvenc_temporal_aq", False)):
            args.extend(["-temporal-aq", "1"])
        bframes = int(getattr(cfg, "video_nvenc_bframes", 0) or 0)
        if bframes > 0:
            args.extend(["-bf", str(bframes)])
        b_ref_mode = str(getattr(cfg, "video_nvenc_b_ref_mode", "") or "").strip()
        if b_ref_mode:
            args.extend(["-b_ref_mode", b_ref_mode])
        return args
    raise ValueError(f"Unsupported video encoder: {encoder}")


def build_ffmpeg_args(
    cfg: Any,
    *,
    x11_input: str,
    pulse_source: str,
    encoder_profile: dict[str, object],
) -> list[str]:
    video_bitrate = str(encoder_profile.get("video_bitrate") or cfg.video_bitrate)
    video_maxrate = str(encoder_profile.get("video_maxrate") or cfg.video_maxrate)
    video_bufsize = str(encoder_profile.get("video_bufsize") or cfg.video_bufsize)
    audio_bitrate = str(encoder_profile.get("audio_bitrate") or cfg.audio_bitrate)
    return [
        "ffmpeg",
        "-nostdin",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-fflags",
        "+genpts",
        "-thread_queue_size",
        "2048",
        "-f",
        "x11grab",
        "-draw_mouse",
        str(cfg.draw_mouse),
        "-framerate",
        str(cfg.frame_rate),
        "-video_size",
        cfg.video_size,
        "-use_wallclock_as_timestamps",
        "1",
        "-i",
        x11_input,
        "-thread_queue_size",
        str(cfg.audio_queue_size),
        "-f",
        "pulse",
        "-i",
        pulse_source,
        "-filter_complex",
        build_filter(cfg.output_size),
        "-af",
        cfg.audio_filter,
        *build_video_encoder_args(cfg),
        "-pix_fmt",
        "yuv420p",
        "-r",
        str(cfg.frame_rate),
        "-g",
        str(cfg.frame_rate * 2),
        "-keyint_min",
        str(cfg.frame_rate * 2),
        "-sc_threshold",
        "0",
        "-b:v",
        video_bitrate,
        "-maxrate",
        video_maxrate,
        "-bufsize",
        video_bufsize,
        "-c:a",
        "aac",
        "-b:a",
        audio_bitrate,
        "-ar",
        str(cfg.audio_sample_rate),
        "-ac",
        "2",
        *build_output_args(cfg),
    ]
