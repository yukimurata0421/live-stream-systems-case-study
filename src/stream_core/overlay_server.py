#!/usr/bin/env python3
"""Serve the stream overlay and same-origin proxy ADS-B JSON."""

from __future__ import annotations

import argparse
import http.server
import json
import math
import os
import socketserver
import time
import urllib.error
import urllib.parse
import urllib.request
from functools import partial
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve()
BASE_DIR = SCRIPT_PATH.parents[2]
ActualRangeRecord = dict[str, object]


class OverlayHandler(http.server.SimpleHTTPRequestHandler):
    stream1090_url = "http://stream1090.lan/stream1090/"
    now_playing_file = Path("now_playing.txt")
    now_playing_json_file = Path("now_playing.json")
    actual_range_supplement_file = Path("/dev/shm/adsb-streamnew/overlay_actual_range_supplement.json")
    actual_range_supplement_hours = 24.0
    actual_range_max_nmi = 500.0
    actual_range_receiver_height_ft = 0.0
    # Keep one bogus decoded position from turning into a day-long range spike.
    actual_range_los_accept_ratio = 1.20
    actual_range_los_quarantine_ratio = 1.25
    actual_range_los_quarantine_repeat_count = 2
    actual_range_neighbor_support_degrees = 2
    actual_range_neighbor_support_margin_nmi = 20.0
    actual_range_aircraft_max_seen_pos_sec = 120.0

    def do_GET(self) -> None:
        if self.handle_overlay_request(send_body=True):
            return
        super().do_GET()

    def do_HEAD(self) -> None:
        if self.handle_overlay_request(send_body=False):
            return
        super().do_HEAD()

    def handle_overlay_request(self, *, send_body: bool) -> bool:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/now_playing.txt":
            self.serve_now_playing_text(send_body=send_body)
            return True
        if parsed.path == "/now_playing.json":
            self.serve_now_playing_json(send_body=send_body)
            return True
        if parsed.path == "/stream1090" or parsed.path.startswith("/stream1090/"):
            self.proxy_stream1090(parsed, send_body=send_body)
            return True
        if parsed.path in {"/adsb/aircraft.json", "/adsb/receiver.json"}:
            self.proxy_adsb_json(parsed.path.rsplit("/", 1)[-1], send_body=send_body)
            return True
        return False

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def serve_now_playing_text(self, *, send_body: bool = True) -> None:
        try:
            body = self.now_playing_file.read_bytes()
            status = 200
        except OSError:
            body = b""
            status = 404

        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def serve_now_playing_json(self, *, send_body: bool = True) -> None:
        try:
            body = self.now_playing_json_file.read_bytes()
            status = 200
        except OSError:
            fallback = self.fallback_now_playing_json()
            if fallback is None:
                body = b""
                status = 404
            else:
                body = fallback
                status = 200

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def fallback_now_playing_json(self) -> bytes | None:
        try:
            title_line = self.now_playing_file.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        if not title_line:
            return None
        payload = {
            "schema": "now_playing_overlay_fallback/v1",
            "status": "playing",
            "now_playing": {
                "title": title_line,
                "title_line": title_line,
            },
        }
        return (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")

    def proxy_adsb_json(self, filename: str, *, send_body: bool = True) -> None:
        base = self.stream1090_url.rstrip("/") + "/"
        url = urllib.parse.urljoin(base, "data/" + filename)
        try:
            with urllib.request.urlopen(url, timeout=3) as res:
                body = res.read()
                status = res.status
                content_type = res.headers.get("Content-Type", "application/json")
        except (urllib.error.URLError, TimeoutError) as exc:
            body = ('{"error":"%s"}' % str(exc).replace('"', "'")).encode("utf-8")
            status = 502
            content_type = "application/json"

        if status == 200 and filename == "receiver.json":
            body = self.sanitize_receiver_json(body)
            content_type = "application/json"

        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def proxy_stream1090(self, parsed: urllib.parse.ParseResult, *, send_body: bool = True) -> None:
        base = self.stream1090_url.rstrip("/") + "/"
        rel_path = parsed.path.removeprefix("/stream1090").lstrip("/")
        url = urllib.parse.urljoin(base, rel_path)
        if parsed.query:
            url += "?" + parsed.query

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "stream-overlay-proxy/1.0"})
            with urllib.request.urlopen(req, timeout=8) as res:
                body = res.read()
                status = res.status
                content_type = res.headers.get("Content-Type", "application/octet-stream")
        except (urllib.error.URLError, TimeoutError) as exc:
            body = ('{"error":"%s"}' % str(exc).replace('"', "'")).encode("utf-8")
            status = 502
            content_type = "application/json"

        if "text/html" in content_type:
            body = self.inject_stream1090_css(body)
            content_type = "text/html; charset=utf-8"
        elif status == 200 and rel_path == "config.js":
            body = self.inject_stream1090_privacy_config(body)
            content_type = "application/javascript; charset=utf-8"
        elif status == 200 and rel_path == "data/outline.json":
            body = self.augment_actual_range_outline(body, base)
            content_type = "application/json"

        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    @staticmethod
    def inject_stream1090_css(body: bytes) -> bytes:
        text = body.decode("utf-8", errors="replace")
        injected = """
<style id="stream_overlay_proxy_css">
html,body,#map_container,#map_canvas,.ol-viewport{cursor:none !important;}
.ol-attribution,.ol-scale-line,#stream_plane_counter{display:none !important;visibility:hidden !important;opacity:0 !important;pointer-events:none !important;}
#update_error,#update_error_detail,#generic_error,#generic_error_detail,#js_error,#js_error_detail,#timers_paused,.error_box{display:none !important;visibility:hidden !important;opacity:0 !important;pointer-events:none !important;}
#stream_footer_notice{right:14px !important;bottom:10px !important;top:auto !important;}
</style>
"""
        if "</head>" in text:
            text = text.replace("</head>", injected + "</head>", 1)
        else:
            text = injected + text
        return text.encode("utf-8")

    @staticmethod
    def sanitize_receiver_json(body: bytes) -> bytes:
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return body
        if not isinstance(payload, dict):
            return body
        payload.pop("lat", None)
        payload.pop("lon", None)
        payload["receiver_location_hidden"] = True
        return (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")

    @staticmethod
    def inject_stream1090_privacy_config(body: bytes) -> bytes:
        text = body.decode("utf-8", errors="replace")
        injected = """

// stream-overlay privacy: hide the receiver position marker while preserving dashed coverage/range guides.
SiteShow = false;
SiteCirclesLineDash = [8, 4];
"""
        if "stream-overlay privacy" in text:
            return text.encode("utf-8")
        return (text.rstrip() + injected + "\n").encode("utf-8")

    @classmethod
    def augment_actual_range_outline(cls, body: bytes, stream1090_base_url: str) -> bytes:
        try:
            outline = json.loads(body.decode("utf-8"))
            aircraft = cls.fetch_json(urllib.parse.urljoin(stream1090_base_url, "data/aircraft.json"), timeout=2)
            receiver = cls.fetch_json(urllib.parse.urljoin(stream1090_base_url, "data/receiver.json"), timeout=2)
            merged = cls.merge_actual_range_outline(outline, aircraft, receiver, time.time())
            return (json.dumps(merged, separators=(",", ":")) + "\n").encode("utf-8")
        except Exception:
            return body

    @staticmethod
    def fetch_json(url: str, timeout: float) -> object:
        req = urllib.request.Request(url, headers={"User-Agent": "stream-overlay-proxy/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return json.loads(res.read().decode("utf-8"))

    @classmethod
    def merge_actual_range_outline(
        cls,
        outline: object,
        aircraft: object,
        receiver: object,
        now_ts: float,
    ) -> object:
        if not isinstance(outline, dict) or not isinstance(aircraft, dict) or not isinstance(receiver, dict):
            return outline
        site_lat = receiver.get("lat")
        site_lon = receiver.get("lon")
        if not isinstance(site_lat, (int, float)) or not isinstance(site_lon, (int, float)):
            return outline

        receiver_height_ft = cls.receiver_height_ft(receiver)
        base_records = cls.outline_records_by_bearing(outline, float(site_lat), float(site_lon))
        supplement = cls.load_actual_range_supplement(now_ts)
        cls.update_actual_range_supplement(
            supplement,
            aircraft,
            float(site_lat),
            float(site_lon),
            now_ts,
            receiver_height_ft,
        )
        supplement = cls.prune_actual_range_supplement(supplement, now_ts)
        supplement = cls.prune_rejected_actual_range_supplement(supplement, receiver_height_ft)
        mergeable_supplement = cls.filter_actual_range_supplement(supplement, base_records, receiver_height_ft)

        changed = False
        for key, record in mergeable_supplement.items():
            base = base_records.get(key)
            record_distance_m = float(record["distance_m"])
            if not base or record_distance_m > float(base["distance_m"]):
                base_records[key] = {
                    "lat": float(record["lat"]),
                    "lon": float(record["lon"]),
                    "alt": record.get("alt", 0),
                    "distance_m": record_distance_m,
                }
                changed = True
        cls.save_actual_range_supplement(supplement)

        if not changed:
            return outline

        points = [
            [round(float(rec["lat"]), 4), round(float(rec["lon"]), 4), int(rec.get("alt") or 0)]
            for _key, rec in sorted(base_records.items(), key=lambda item: int(item[0]))
        ]
        outline.setdefault("actualRange", {}).setdefault("last24h", {})["points"] = points
        return outline

    @staticmethod
    def outline_records_by_bearing(outline: dict, site_lat: float, site_lon: float) -> dict[str, ActualRangeRecord]:
        points = outline.get("actualRange", {}).get("last24h", {}).get("points", [])
        records: dict[str, ActualRangeRecord] = {}
        if not isinstance(points, list):
            return records
        for point in points:
            if not isinstance(point, list) or len(point) < 2:
                continue
            lat, lon = point[0], point[1]
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                continue
            distance_m, bearing_deg = range_distance_bearing_m(site_lat, site_lon, float(lat), float(lon))
            key = str(int(round(bearing_deg)) % 360)
            alt = point[2] if len(point) > 2 and isinstance(point[2], (int, float)) else 0
            if key not in records or distance_m > float(records[key]["distance_m"]):
                records[key] = {"lat": float(lat), "lon": float(lon), "alt": alt, "distance_m": distance_m}
        return records

    @classmethod
    def load_actual_range_supplement(cls, now_ts: float) -> dict[str, ActualRangeRecord]:
        try:
            raw = json.loads(cls.actual_range_supplement_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(raw, dict) or raw.get("schema") != "overlay_actual_range_supplement/v1":
            return {}
        records = raw.get("records", {})
        if not isinstance(records, dict):
            return {}
        valid: dict[str, ActualRangeRecord] = {}
        for key, rec in records.items():
            if not isinstance(key, str) or not isinstance(rec, dict):
                continue
            if not all(isinstance(rec.get(k), (int, float)) for k in ("lat", "lon", "distance_m", "updated_ts")):
                continue
            valid[key] = rec
        return cls.prune_actual_range_supplement(valid, now_ts)

    @classmethod
    def save_actual_range_supplement(cls, records: dict[str, ActualRangeRecord]) -> None:
        try:
            cls.actual_range_supplement_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = cls.actual_range_supplement_file.with_suffix(".tmp")
            payload = {
                "schema": "overlay_actual_range_supplement/v1",
                "updated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "records": records,
            }
            tmp.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")
            tmp.replace(cls.actual_range_supplement_file)
        except OSError:
            return

    @classmethod
    def prune_actual_range_supplement(
        cls,
        records: dict[str, ActualRangeRecord],
        now_ts: float,
    ) -> dict[str, ActualRangeRecord]:
        max_age = max(60.0, cls.actual_range_supplement_hours * 3600.0)
        return {
            key: rec
            for key, rec in records.items()
            if now_ts - float(rec.get("updated_ts", 0.0)) <= max_age
        }

    @classmethod
    def filter_actual_range_supplement(
        cls,
        records: dict[str, ActualRangeRecord],
        base_records: dict[str, ActualRangeRecord],
        receiver_height_ft: float | None = None,
    ) -> dict[str, ActualRangeRecord]:
        if receiver_height_ft is None:
            receiver_height_ft = cls.actual_range_receiver_height_ft
        return {
            key: rec
            for key, rec in records.items()
            if cls.is_actual_range_supplement_mergeable(key, rec, base_records, receiver_height_ft)
        }

    @classmethod
    def prune_rejected_actual_range_supplement(
        cls,
        records: dict[str, ActualRangeRecord],
        receiver_height_ft: float,
    ) -> dict[str, ActualRangeRecord]:
        return {
            key: rec
            for key, rec in records.items()
            if cls.is_actual_range_supplement_plausible(key, float(rec["distance_m"]), rec.get("alt", 0), receiver_height_ft)
        }

    @classmethod
    def is_actual_range_supplement_mergeable(
        cls,
        key: str,
        rec: ActualRangeRecord,
        base_records: dict[str, ActualRangeRecord],
        receiver_height_ft: float,
    ) -> bool:
        distance_m = float(rec["distance_m"])
        los_status = cls.actual_range_los_status(distance_m, rec.get("alt", 0), receiver_height_ft)
        if los_status == "accept":
            return True
        if los_status != "quarantine":
            return False

        repeat_count = cls.safe_int(rec.get("los_repeat_count", 0))
        if repeat_count >= cls.actual_range_los_quarantine_repeat_count:
            return True
        return cls.has_actual_range_neighbor_support(key, distance_m, base_records)

    @classmethod
    def is_actual_range_supplement_plausible(
        cls,
        key: str,
        distance_m: float,
        alt: object,
        receiver_height_ft: float | None = None,
    ) -> bool:
        if distance_m <= 0:
            return False
        max_distance_m = cls.actual_range_max_nmi * 1852.0
        if distance_m > max_distance_m:
            return False

        try:
            bearing = int(key) % 360
        except ValueError:
            return False

        if receiver_height_ft is None:
            receiver_height_ft = cls.actual_range_receiver_height_ft
        return cls.actual_range_los_status(distance_m, alt, receiver_height_ft) != "reject"

    @classmethod
    def has_actual_range_neighbor_support(
        cls,
        key: str,
        distance_m: float,
        base_records: dict[str, ActualRangeRecord],
    ) -> bool:
        try:
            bearing = int(key) % 360
        except ValueError:
            return False
        neighbor_distances = [
            float(base_records[str((bearing + offset) % 360)]["distance_m"])
            for offset in range(-cls.actual_range_neighbor_support_degrees, cls.actual_range_neighbor_support_degrees + 1)
            if str((bearing + offset) % 360) in base_records
        ]
        if not neighbor_distances:
            return False
        supported_distance_m = max(neighbor_distances) + cls.actual_range_neighbor_support_margin_nmi * 1852.0
        return distance_m <= supported_distance_m

    @classmethod
    def actual_range_los_status(cls, distance_m: float, alt: object, receiver_height_ft: float) -> str:
        aircraft_alt_ft = cls.coerce_non_negative_float(alt)
        receiver_height_ft = max(0.0, receiver_height_ft)
        radio_los_nmi = cls.radio_los_nmi(aircraft_alt_ft, receiver_height_ft)
        if radio_los_nmi <= 0:
            return "reject"
        distance_nmi = distance_m / 1852.0
        los_ratio = distance_nmi / radio_los_nmi
        if los_ratio <= cls.actual_range_los_accept_ratio:
            return "accept"
        if los_ratio <= cls.actual_range_los_quarantine_ratio:
            return "quarantine"
        return "reject"

    @staticmethod
    def radio_los_nmi(aircraft_alt_ft: float, receiver_height_ft: float) -> float:
        return 1.23 * (math.sqrt(max(0.0, aircraft_alt_ft)) + math.sqrt(max(0.0, receiver_height_ft)))

    @classmethod
    def receiver_height_ft(cls, receiver: dict) -> float:
        for key in ("receiver_height_ft", "antenna_height_ft", "site_alt_ft", "alt_ft", "height_ft"):
            value = receiver.get(key)
            if isinstance(value, (int, float)) and value >= 0:
                return float(value)
        for key in ("receiver_height_m", "antenna_height_m", "site_alt_m", "alt_m", "height_m"):
            value = receiver.get(key)
            if isinstance(value, (int, float)) and value >= 0:
                return float(value) * 3.28084
        return max(0.0, float(cls.actual_range_receiver_height_ft))

    @staticmethod
    def coerce_non_negative_float(value: object) -> float:
        if isinstance(value, (int, float)) and value > 0:
            return float(value)
        return 0.0

    @staticmethod
    def safe_int(value: object) -> int:
        if isinstance(value, bool):
            return 0
        if isinstance(value, (int, float)):
            return int(value)
        return 0

    @classmethod
    def update_actual_range_supplement(
        cls,
        records: dict[str, ActualRangeRecord],
        aircraft: dict,
        site_lat: float,
        site_lon: float,
        now_ts: float,
        receiver_height_ft: float,
    ) -> None:
        max_distance_m = cls.actual_range_max_nmi * 1852.0
        for ac in aircraft.get("aircraft", []):
            if not isinstance(ac, dict):
                continue
            lat = ac.get("lat")
            lon = ac.get("lon")
            seen_pos = ac.get("seen_pos", 0)
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                continue
            if isinstance(seen_pos, (int, float)) and seen_pos > cls.actual_range_aircraft_max_seen_pos_sec:
                continue
            distance_m, bearing_deg = range_distance_bearing_m(site_lat, site_lon, float(lat), float(lon))
            if distance_m <= 0 or distance_m > max_distance_m:
                continue
            key = str(int(round(bearing_deg)) % 360)
            alt = ac.get("alt_baro")
            if not isinstance(alt, (int, float)):
                alt = ac.get("alt_geom", 0)
            los_status = cls.actual_range_los_status(distance_m, alt, receiver_height_ft)
            if los_status == "reject":
                continue
            alt_ft = int(alt) if isinstance(alt, (int, float)) else 0
            current = records.get(key)
            repeat_count = 1
            if current and los_status == "quarantine":
                repeat_count = cls.safe_int(current.get("los_repeat_count", 0)) + 1
            if not current or distance_m > float(current.get("distance_m", 0.0)):
                radio_los_nmi = cls.radio_los_nmi(cls.coerce_non_negative_float(alt), receiver_height_ft)
                records[key] = {
                    "lat": float(lat),
                    "lon": float(lon),
                    "alt": alt_ft,
                    "distance_m": distance_m,
                    "updated_ts": now_ts,
                    "radio_los_nmi": radio_los_nmi,
                    "los_ratio": distance_m / 1852.0 / radio_los_nmi if radio_los_nmi > 0 else 0.0,
                    "los_status": los_status,
                    "los_repeat_count": repeat_count,
                }
            elif los_status == "quarantine":
                current["updated_ts"] = now_ts
                current["los_status"] = los_status
                current["los_repeat_count"] = repeat_count


def range_distance_bearing_m(lat1: float, lon1: float, lat2: float, lon2: float) -> tuple[float, float]:
    to_rad = math.pi / 180.0
    phi1 = lat1 * to_rad
    phi2 = lat2 * to_rad
    d_lat = (lat2 - lat1) * to_rad
    d_lon = (lon2 - lon1) * to_rad
    a = math.sin(d_lat / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lon / 2.0) ** 2
    distance_m = 6_371_000.0 * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    y = math.sin(d_lon) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(d_lon)
    bearing_deg = (math.degrees(math.atan2(y, x)) + 360.0) % 360.0
    return distance_m, bearing_deg


def default_now_playing_json_file(now_playing_file: Path) -> Path:
    explicit = os.environ.get("NOW_PLAYING_JSON") or os.environ.get("NOW_PLAYING_SNAPSHOT_FILE")
    if explicit:
        return Path(explicit)
    if now_playing_file.name == "now_playing.txt":
        return now_playing_file.parent / "overlay" / "now_playing.json"
    return BASE_DIR / "ui" / "overlay" / "now_playing.json"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.environ.get("OVERLAY_BIND_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("OVERLAY_PORT", "18080")))
    parser.add_argument("--directory", default=os.environ.get("OVERLAY_DIR", str(BASE_DIR / "ui" / "overlay")))
    parser.add_argument("--stream1090-url", default=os.environ.get("STREAM1090_URL", OverlayHandler.stream1090_url))
    parser.add_argument(
        "--now-playing-file",
        default=os.environ.get("NOW_PLAYING_FILE", str(BASE_DIR / "now_playing.txt")),
    )
    parser.add_argument(
        "--now-playing-json-file",
        default=os.environ.get("NOW_PLAYING_JSON") or os.environ.get("NOW_PLAYING_SNAPSHOT_FILE"),
    )
    parser.add_argument(
        "--actual-range-supplement-file",
        default=os.environ.get(
            "OVERLAY_ACTUAL_RANGE_SUPPLEMENT_FILE",
            "/dev/shm/adsb-streamnew/overlay_actual_range_supplement.json",
        ),
    )
    parser.add_argument(
        "--actual-range-supplement-hours",
        type=float,
        default=float(os.environ.get("OVERLAY_ACTUAL_RANGE_SUPPLEMENT_HOURS", "24")),
    )
    parser.add_argument(
        "--actual-range-receiver-height-ft",
        type=float,
        default=float(os.environ.get("OVERLAY_ACTUAL_RANGE_RECEIVER_HEIGHT_FT", "0")),
    )
    args = parser.parse_args()

    OverlayHandler.stream1090_url = args.stream1090_url
    overlay_dir = Path(args.directory).resolve()
    OverlayHandler.now_playing_file = Path(args.now_playing_file).resolve()
    OverlayHandler.now_playing_json_file = (
        Path(args.now_playing_json_file).resolve()
        if args.now_playing_json_file
        else default_now_playing_json_file(OverlayHandler.now_playing_file).resolve()
    )
    OverlayHandler.actual_range_supplement_file = Path(args.actual_range_supplement_file).resolve()
    OverlayHandler.actual_range_supplement_hours = args.actual_range_supplement_hours
    OverlayHandler.actual_range_receiver_height_ft = args.actual_range_receiver_height_ft
    handler = partial(OverlayHandler, directory=str(overlay_dir))
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer((args.host, args.port), handler) as httpd:
        httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
