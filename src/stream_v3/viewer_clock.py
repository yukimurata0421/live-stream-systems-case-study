"""Memory-only image analysis and the independent viewer clock observation contract."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

SCHEMA = "stream_v3.viewer_clock_probe.v1"
PLATFORMS = ("youtube", "twitch")
CLOCK_STATES = (
    "WARMUP", "ADVANCING_PROBABLE", "ADVANCING_CONFIRMED", "STALLED_SUSPECT",
    "STALLED_CONFIRMED", "BLACK_FRAME", "OCR_UNCERTAIN", "UNKNOWN",
)
TIME_RE = re.compile(r"(?<!\d)([0-2]\d):([0-5]\d):([0-5]\d)(?!\d)")


def utc_timestamp(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class GrayImage:
    width: int
    height: int
    pixels: bytes

    def __post_init__(self) -> None:
        if not (0 < self.width <= 1920 and 0 < self.height <= 1080):
            raise ValueError("invalid_image_dimensions")
        if len(self.pixels) != self.width * self.height:
            raise ValueError("invalid_pixel_count")

    @classmethod
    def from_pgm(cls, content: bytes) -> GrayImage:
        # FFmpeg's P5 output has no comments. Consume exactly the header terminator;
        # stripping whitespace here would corrupt images whose first pixel is 10/32.
        match = re.match(rb"P5\s+(\d+)\s+(\d+)\s+255\r?\n", content)
        if not match:
            raise ValueError("invalid_pgm")
        return cls(int(match[1]), int(match[2]), content[match.end():])

    def pgm(self) -> bytes:
        return f"P5\n{self.width} {self.height}\n255\n".encode() + self.pixels

    def crop(self, roi: tuple[float, float, float, float]) -> GrayImage:
        x, y, w, h = roi
        if not all(math.isfinite(v) for v in roi) or not (0 <= x < 1 and 0 <= y < 1 and w > 0 and h > 0 and x + w <= 1 and y + h <= 1):
            raise ValueError("invalid_roi")
        left, top = round(x * self.width), round(y * self.height)
        right, bottom = min(self.width, round((x + w) * self.width)), min(self.height, round((y + h) * self.height))
        if right - left < 8 or bottom - top < 4:
            raise ValueError("roi_too_small")
        return GrayImage(right - left, bottom - top, b"".join(
            self.pixels[row * self.width + left:row * self.width + right] for row in range(top, bottom)
        ))

    def reduced(self, width: int = 64, height: int = 36) -> GrayImage:
        # Used only to distinguish clock-only stasis from possible whole-frame stasis.
        return GrayImage(width, height, bytes(
            self.pixels[min(self.height - 1, y * self.height // height) * self.width + min(self.width - 1, x * self.width // width)]
            for y in range(height) for x in range(width)
        ))

    def ocr_input(self) -> GrayImage:
        # Enlarge and invert the light-on-dark clock. No normal image touches disk.
        scale, border = 4, 12
        width = self.width * scale + border * 2
        rows = [bytes([255]) * width] * border
        for y in range(self.height):
            row = bytes([255]) * border + b"".join(bytes([255 - p]) * scale for p in self.pixels[y * self.width:(y + 1) * self.width]) + bytes([255]) * border
            rows.extend([row] * scale)
        rows.extend([bytes([255]) * width] * border)
        return GrayImage(width, len(rows), b"".join(rows))


def image_difference(left: GrayImage, right: GrayImage, *, noise_floor: int = 12) -> float | None:
    if (left.width, left.height) != (right.width, right.height):
        return None
    # Ignore small compression differences, then report changed-pixel fraction.
    return sum(abs(a - b) > noise_floor for a, b in zip(left.pixels, right.pixels)) / len(left.pixels)


@dataclass(frozen=True)
class ClockReading:
    text: str = ""
    confidence: float = 0.0
    error: str = ""

    def seconds(self, minimum_confidence: float) -> int | None:
        match = TIME_RE.fullmatch(self.text)
        if not match or not math.isfinite(self.confidence) or not minimum_confidence <= self.confidence <= 100:
            return None
        hour, minute, second = map(int, match.groups())
        return hour * 3600 + minute * 60 + second if hour < 24 else None


@dataclass(frozen=True)
class Capture:
    frame: GrayImage
    identity: str  # Public content identity; never a signed playback URL.
    captured_epoch: float
    captured_monotonic: float
    codec: str = "unknown"
    duration_seconds: float = 0.0
    source_width: int | None = None
    source_height: int | None = None


@dataclass(frozen=True)
class AnalysisConfig:
    roi: tuple[float, float, float, float]
    change_threshold: float = 0.015
    noise_floor: int = 12
    audit_interval: int = 10
    minimum_confidence: float = 70.0
    stall_confirmations: int = 2
    max_comparison_gap_seconds: float = 180.0
    black_pixel_threshold: int = 12
    black_ratio_threshold: float = 0.995


class ClockAnalyzer:
    def __init__(self, platform: str, config: AnalysisConfig) -> None:
        self.platform, self.config = platform, config
        self.previous: tuple[Capture, GrayImage, GrayImage] | None = None
        self.comparisons = self.stalls = self.failures = self.anomalies = 0
        self.needs_ocr = True
        self.last_capture_epoch: float | None = None

    def unknown(self, reason: str, *, now: float) -> dict[str, Any]:
        self.previous = None
        self.stalls = self.comparisons = 0
        self.needs_ocr = True
        self.failures += 1
        self.anomalies = 0
        return {
            "platform": self.platform, "checked_at_utc": utc_timestamp(now),
            "captured_at_utc": utc_timestamp(self.last_capture_epoch) if self.last_capture_epoch is not None else None,
            "capture_ok": False, "clock_state": "UNKNOWN",
            "ocr_verified": False, "consecutive_failures": self.failures,
            "consecutive_stalls": 0, "consecutive_anomalies": 0,
            "sample_age_seconds": max(0, now - self.last_capture_epoch) if self.last_capture_epoch is not None else None,
            "reason": reason, "frame_state": "UNKNOWN",
        }

    def analyze(self, capture: Capture, ocr: Callable[[GrayImage], ClockReading], *, now: float) -> tuple[dict[str, Any], tuple[GrayImage, GrayImage] | None]:
        cfg = self.config
        try:
            roi = capture.frame.crop(cfg.roi)
        except ValueError:
            return self.unknown("roi_invalid_or_too_small", now=now), None
        # Retain only the ROI and a small full-frame summary between cycles.
        compact = Capture(capture.frame.reduced(), capture.identity, capture.captured_epoch, capture.captured_monotonic, capture.codec)
        previous = self.previous
        self.previous = (compact, roi, compact.frame)
        self.last_capture_epoch = capture.captured_epoch
        self.failures = 0
        row: dict[str, Any] = {
            "platform": self.platform, "checked_at_utc": utc_timestamp(now),
            "captured_at_utc": utc_timestamp(capture.captured_epoch), "capture_ok": True,
            "clock_state": "WARMUP", "ocr_verified": False, "consecutive_failures": 0,
            "sample_age_seconds": max(0.0, now - capture.captured_epoch), "reason": "comparison_unavailable",
            "capture_width": capture.frame.width, "capture_height": capture.frame.height,
            "codec": capture.codec, "capture_duration_seconds": round(capture.duration_seconds, 3),
            "source_width": capture.source_width, "source_height": capture.source_height,
            "image_difference": None, "frame_state": "WARMUP", "clock_anomaly": None,
        }
        evidence = None
        black = sum(p <= cfg.black_pixel_threshold for p in capture.frame.pixels) / len(capture.frame.pixels) >= cfg.black_ratio_threshold
        gap = capture.captured_monotonic - previous[0].captured_monotonic if previous else 0
        comparable = previous is not None and previous[0].identity == capture.identity and 0 < gap <= cfg.max_comparison_gap_seconds and (previous[1].width, previous[1].height) == (roi.width, roi.height)
        if black:
            row.update(clock_state="BLACK_FRAME", reason="black_frame", frame_state="BLACK_FRAME")
            self.previous = None
            self.stalls = self.comparisons = 0
            self.needs_ocr = True
            evidence = (roi, roi)
        elif not comparable:
            self.stalls = self.comparisons = 0
            self.needs_ocr = True
        else:
            assert previous is not None
            self.comparisons += 1
            delta = image_difference(previous[1], roi, noise_floor=cfg.noise_floor)
            full_delta = image_difference(previous[2], compact.frame, noise_floor=cfg.noise_floor)
            row.update(image_difference=delta, frame_image_difference=full_delta, frame_state="CHANGING" if full_delta is not None and full_delta >= cfg.change_threshold else "STATIC_SUSPECT")
            changed = delta is not None and delta >= cfg.change_threshold
            audit = (self.comparisons - 1) % cfg.audit_interval == 0
            if changed and not audit and not self.needs_ocr and self.stalls == 0:
                self.stalls = 0
                row.update(clock_state="ADVANCING_PROBABLE", reason="roi_changed")
            else:
                readings = (ocr(previous[1]), ocr(roi))
                seconds = tuple(r.seconds(cfg.minimum_confidence) for r in readings)
                row.update(ocr_trigger="audit" if changed else "stalled_suspect", ocr_previous=readings[0].text, ocr_current=readings[1].text, ocr_confidence_previous=readings[0].confidence, ocr_confidence_current=readings[1].confidence)
                if None in seconds:
                    self.stalls = 0
                    self.needs_ocr = True
                    row.update(clock_state="OCR_UNCERTAIN", reason="ocr_unreliable", ocr_error=next((r.error for r in readings if r.error), "low_confidence_or_invalid_time"))
                else:
                    before, after = seconds
                    assert before is not None and after is not None
                    forward = (after - before) % 86400
                    row.update(ocr_verified=True, clock_delta_seconds=forward if forward <= 43200 else forward - 86400)
                    if forward == 0:
                        self.stalls += 1
                        self.needs_ocr = True
                        row.update(clock_state="STALLED_CONFIRMED" if self.stalls >= cfg.stall_confirmations else "STALLED_SUSPECT", reason="ocr_time_unchanged")
                    elif forward <= min(600, max(10, gap * 2 + 5)):
                        self.stalls = 0
                        self.needs_ocr = False
                        row.update(clock_state="ADVANCING_CONFIRMED", reason="ocr_time_advanced")
                    else:
                        self.stalls = 0
                        self.needs_ocr = True
                        row.update(clock_state="OCR_UNCERTAIN", reason="clock_time_regressed" if forward > 43200 else "clock_time_jump", clock_anomaly="time_regression" if forward > 43200 else "time_jump")
                if row["clock_state"] not in ("ADVANCING_CONFIRMED",):
                    evidence = (previous[1], roi)
        abnormal = row["clock_state"] in ("BLACK_FRAME", "STALLED_SUSPECT", "STALLED_CONFIRMED", "OCR_UNCERTAIN")
        self.anomalies = self.anomalies + 1 if abnormal else 0
        row.update(consecutive_stalls=self.stalls, consecutive_anomalies=self.anomalies)
        return row, evidence


def target_facts(row: dict[str, Any], *, now: float, freshness_seconds: float = 180) -> dict[str, Any]:
    """Fail-closed facts for metric/Monitoring consumers; no recovery decision."""
    def age(key: str) -> float | None:
        try:
            stamp = datetime.fromisoformat(row[key].replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                return None
            value = now - stamp.timestamp()
            return value if math.isfinite(value) and value >= 0 else None
        except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
            return None
    checked_age, sample_age = age("checked_at_utc"), age("captured_at_utc")
    state = row.get("clock_state")
    fresh = checked_age is not None and checked_age <= freshness_seconds
    capture_ok = row.get("capture_ok") is True and sample_age is not None and sample_age <= freshness_seconds
    valid = fresh and capture_ok and state in CLOCK_STATES
    clock_fact = "unknown"
    if valid and (state == "ADVANCING_PROBABLE" or (state == "ADVANCING_CONFIRMED" and row.get("ocr_verified") is True)):
        clock_fact = "good"
    elif valid and state in ("STALLED_CONFIRMED", "BLACK_FRAME"):
        if state == "BLACK_FRAME" or (row.get("ocr_verified") is True and isinstance(row.get("consecutive_stalls"), int) and row["consecutive_stalls"] >= 2):
            clock_fact = "bad"
    return {"platform": row.get("platform"), "capture_status": "good" if fresh and capture_ok else "unknown", "clock_status": clock_fact, "fresh": fresh, "sample_age_seconds": sample_age, "checked_age_seconds": checked_age}


def write_metrics(writer: Any, snapshot: dict[str, Any], *, now: float) -> None:
    targets = snapshot.get("targets") if snapshot.get("schema") == SCHEMA else {}
    targets = targets if isinstance(targets, dict) else {}
    for platform in PLATFORMS:
        row = targets.get(platform, {})
        row = row if isinstance(row, dict) and row.get("platform") == platform else {}
        facts = target_facts(row, now=now)
        labels = {"platform": platform}
        for suffix, value in (
            ("sample_available", int(facts["checked_age_seconds"] is not None)),
            ("sample_fresh", int(facts["fresh"])),
            ("sample_age_seconds", facts["sample_age_seconds"] if facts["sample_age_seconds"] is not None else -1),
            ("capture_ok", int(facts["capture_status"] == "good")),
            ("ocr_verified", int(facts["fresh"] and row.get("ocr_verified") is True)),
            ("consecutive_failures", row.get("consecutive_failures", 0)),
            ("consecutive_stalls", row.get("consecutive_stalls", 0)),
        ):
            writer.metric(f"stream_v3_viewer_clock_{suffix}", value, labels=labels)
        state = row.get("clock_state", "UNKNOWN") if facts["fresh"] else "UNKNOWN"
        if state not in CLOCK_STATES:
            state = "UNKNOWN"
        for candidate in CLOCK_STATES:
            writer.metric("stream_v3_viewer_clock_state", int(candidate == state), labels={**labels, "state": candidate})
