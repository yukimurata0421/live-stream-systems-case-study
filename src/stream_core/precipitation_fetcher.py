#!/usr/bin/env python3
"""Fetch and process JMA analysis-only precipitation tiles for the stream map.

This process is intentionally independent from the ADS-B renderer. Upstream
weather failures keep the last successful generation on disk; the browser
hides it after the configured stale interval while the stream continues.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import io
import json
import math
import os
import re
import shutil
import tempfile
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

from PIL import Image


SCHEMA = "stream_v3.precipitation.v1"
DEFAULT_METADATA_URL = "https://www.jma.go.jp/bosai/jmatile/data/nowc/targetTimes_N1.json"
DEFAULT_DATA_ROOT_URL = "https://www.jma.go.jp/bosai/jmatile/data/nowc"
DEFAULT_SOURCE_PAGE = "https://www.jma.go.jp/bosai/nowc/"
DEFAULT_BOUNDS = (133.79165750178356, 33.13454570645014, 147.70834249821644, 39.43795428878076)
DEFAULT_TILE_ZOOMS = (6,)
DEFAULT_RETRY_DELAYS_SEC = (15, 45, 120)
USER_AGENT = "stream-v3-precipitation-fetcher/1.0"

# The source palette thresholds are documented by JMA as
# <1, 1-5, 5-10, 10-20, 20-30, 30-50, 50-80, and >=80 mm/h.
# The output uses subdued large-area fills that remain separate from aircraft
# altitude colors. Values below 1 mm/h are intentionally transparent.
PALETTE_RULES: dict[tuple[int, int, int], tuple[tuple[int, int, int], int]] = {
    (255, 255, 255): ((255, 255, 255), 0),
    (242, 242, 255): ((116, 145, 158), 0),
    (160, 210, 255): ((116, 145, 158), 26),
    (33, 140, 255): ((65, 135, 140), 56),
    (0, 65, 255): ((59, 126, 132), 71),
    (250, 245, 0): ((188, 137, 75), 97),
    (255, 153, 0): ((192, 106, 63), 112),
    (255, 40, 0): ((179, 76, 64), 140),
    (180, 0, 104): ((157, 54, 55), 153),
}


@dataclass(frozen=True)
class FetcherConfig:
    output_root: Path
    metadata_url: str
    data_root_url: str
    bounds: tuple[float, float, float, float]
    tile_zooms: tuple[int, ...]
    poll_sec: int = 300
    publish_delay_sec: int = 25
    retry_delays_sec: tuple[int, ...] = DEFAULT_RETRY_DELAYS_SEC
    stale_sec: int = 900
    timeout_sec: float = 20.0
    workers: int = 4
    keep_generations: int = 3


FetchBytes = Callable[[str, float], bytes]


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_jma_time(value: str) -> dt.datetime:
    if not re.fullmatch(r"\d{14}", value):
        raise ValueError(f"invalid JMA timestamp: {value!r}")
    return dt.datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)


def select_latest_analysis(rows: object) -> dict[str, object]:
    if not isinstance(rows, list):
        raise ValueError("JMA target times response is not a list")
    candidates: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        basetime = row.get("basetime")
        validtime = row.get("validtime")
        elements = row.get("elements")
        if not isinstance(basetime, str) or not isinstance(validtime, str):
            continue
        if basetime != validtime or not isinstance(elements, list) or "hrpns" not in elements:
            continue
        parse_jma_time(validtime)
        candidates.append(row)
    if not candidates:
        raise ValueError("JMA response contains no analysis-only hrpns timestamp")
    return max(candidates, key=lambda row: str(row["validtime"]))


def lon_to_tile_x(lon: float, zoom: int) -> int:
    count = 1 << zoom
    return min(count - 1, max(0, int(math.floor((lon + 180.0) / 360.0 * count))))


def lat_to_tile_y(lat: float, zoom: int) -> int:
    count = 1 << zoom
    limited = min(85.05112878, max(-85.05112878, lat))
    radians = math.radians(limited)
    value = (1.0 - math.asinh(math.tan(radians)) / math.pi) / 2.0 * count
    return min(count - 1, max(0, int(math.floor(value))))


def tiles_for_bounds(
    bounds: tuple[float, float, float, float],
    zooms: Iterable[int],
) -> list[tuple[int, int, int]]:
    west, south, east, north = bounds
    if not (-180 <= west < east <= 180 and -85.05112878 <= south < north <= 85.05112878):
        raise ValueError(f"invalid precipitation bounds: {bounds!r}")
    tiles: list[tuple[int, int, int]] = []
    for zoom in sorted(set(zooms)):
        if not 0 <= zoom <= 10:
            raise ValueError(f"unsupported JMA tile zoom: {zoom}")
        x_min = lon_to_tile_x(west, zoom)
        x_max = lon_to_tile_x(east, zoom)
        y_min = lat_to_tile_y(north, zoom)
        y_max = lat_to_tile_y(south, zoom)
        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                tiles.append((zoom, x, y))
    return tiles


def fetch_bytes(url: str, timeout_sec: float) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json,image/png;q=0.9,*/*;q=0.1",
            "Accept-Encoding": "identity",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        return response.read()


def _original_alpha(transparency: object, index: int) -> int:
    if isinstance(transparency, bytes):
        return transparency[index] if index < len(transparency) else 255
    if isinstance(transparency, int):
        return 0 if index == transparency else 255
    return 255


def process_precipitation_tile(source: bytes) -> tuple[bytes, int]:
    with Image.open(io.BytesIO(source)) as image:
        image.load()
        if image.mode == "RGBA":
            color_counts = image.getcolors(maxcolors=256)
            if color_counts is None:
                raise ValueError("JMA RGBA precipitation tile has too many colors")
            replacements: dict[tuple[int, int, int, int], tuple[int, int, int, int]] = {}
            active_pixels = 0
            for count, source_rgba in color_counts:
                source_rgb = source_rgba[:3]
                source_alpha = source_rgba[3]
                if source_alpha == 0:
                    replacements[source_rgba] = (255, 255, 255, 0)
                    continue
                rule = PALETTE_RULES.get(source_rgb)
                if rule is None:
                    raise ValueError(f"unexpected active JMA RGBA color: {source_rgb!r}")
                output_rgb, output_alpha = rule
                replacements[source_rgba] = (*output_rgb, round(output_alpha * source_alpha / 255))
                if output_alpha > 0:
                    active_pixels += count
            output = Image.new("RGBA", image.size)
            pixels = (
                image.get_flattened_data()
                if hasattr(image, "get_flattened_data")
                else image.getdata()
            )
            output.putdata([replacements[pixel] for pixel in pixels])
            encoded = io.BytesIO()
            output.save(encoded, format="PNG", optimize=True)
            return encoded.getvalue(), active_pixels
        if image.mode != "P":
            raise ValueError(f"unexpected JMA precipitation tile mode: {image.mode}")
        palette = list(image.getpalette() or [])
        if len(palette) < 3:
            raise ValueError("JMA precipitation tile has no palette")
        palette.extend([0] * (768 - len(palette)))
        histogram = image.histogram()
        transparency = image.info.get("transparency")
        output_alpha = [0] * 256
        active_pixels = 0

        for index, count in enumerate(histogram[:256]):
            base = index * 3
            source_rgb = tuple(palette[base : base + 3])
            rule = PALETTE_RULES.get(source_rgb)
            if rule is None:
                if count and _original_alpha(transparency, index) > 0:
                    raise ValueError(f"unexpected active JMA palette color: {source_rgb!r}")
                continue
            output_rgb, alpha = rule
            palette[base : base + 3] = output_rgb
            output_alpha[index] = alpha
            if alpha > 0:
                active_pixels += count

        output = image.copy()
        output.putpalette(palette)
        encoded = io.BytesIO()
        output.save(
            encoded,
            format="PNG",
            optimize=True,
            bits=4,
            transparency=bytes(output_alpha),
        )
        return encoded.getvalue(), active_pixels


def count_active_pixels_in_bounds(
    processed: bytes,
    tile: tuple[int, int, int],
    bounds: tuple[float, float, float, float],
) -> int:
    zoom, x, y = tile
    west, south, east, north = bounds
    world_size = float((1 << zoom) * 256)

    def world_x(lon: float) -> float:
        return (lon + 180.0) / 360.0 * world_size

    def world_y(lat: float) -> float:
        radians = math.radians(min(85.05112878, max(-85.05112878, lat)))
        return (1.0 - math.asinh(math.tan(radians)) / math.pi) / 2.0 * world_size

    left = max(0, math.floor(world_x(west) - x * 256))
    right = min(256, math.ceil(world_x(east) - x * 256))
    top = max(0, math.floor(world_y(north) - y * 256))
    bottom = min(256, math.ceil(world_y(south) - y * 256))
    if left >= right or top >= bottom:
        return 0
    with Image.open(io.BytesIO(processed)) as image:
        alpha = image.convert("RGBA").getchannel("A").crop((left, top, right, bottom))
        histogram = alpha.histogram()
    return sum(histogram[1:])


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_status(path: Path) -> dict[str, object] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def tile_url(config: FetcherConfig, basetime: str, validtime: str, tile: tuple[int, int, int]) -> str:
    zoom, x, y = tile
    return (
        f"{config.data_root_url.rstrip('/')}/{basetime}/none/{validtime}"
        f"/surf/hrpns/{zoom}/{x}/{y}.png"
    )


def _fetch_one_tile(
    config: FetcherConfig,
    basetime: str,
    validtime: str,
    tile: tuple[int, int, int],
    fetch: FetchBytes,
) -> tuple[tuple[int, int, int], bytes, int]:
    source = fetch(tile_url(config, basetime, validtime, tile), config.timeout_sec)
    processed, _active_pixels = process_precipitation_tile(source)
    active_pixels = count_active_pixels_in_bounds(processed, tile, config.bounds)
    return tile, processed, active_pixels


def prune_generations(root: Path, keep: int) -> None:
    candidates = sorted(
        (path for path in root.iterdir() if path.is_dir() and re.fullmatch(r"\d{14}", path.name)),
        key=lambda path: path.name,
        reverse=True,
    )
    for obsolete in candidates[max(1, keep) :]:
        shutil.rmtree(obsolete)


def refresh_once(
    config: FetcherConfig,
    *,
    fetch: FetchBytes = fetch_bytes,
    now: Callable[[], dt.datetime] = utc_now,
) -> tuple[dict[str, object], bool]:
    metadata = json.loads(fetch(config.metadata_url, config.timeout_sec).decode("utf-8"))
    selected = select_latest_analysis(metadata)
    basetime = str(selected["basetime"])
    validtime = str(selected["validtime"])
    observed_at = parse_jma_time(validtime)

    config.output_root.mkdir(parents=True, exist_ok=True)
    generations_root = config.output_root / "generations"
    generations_root.mkdir(parents=True, exist_ok=True)
    target = generations_root / validtime
    status_path = config.output_root / "status.json"
    current = read_status(status_path)
    if current and current.get("validtime") == validtime and target.is_dir():
        return current, False

    tiles = tiles_for_bounds(config.bounds, config.tile_zooms)
    stage = Path(tempfile.mkdtemp(prefix=f".{validtime}.", dir=generations_root))
    active_pixels = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, config.workers)) as executor:
            futures = [
                executor.submit(_fetch_one_tile, config, basetime, validtime, tile, fetch)
                for tile in tiles
            ]
            for future in concurrent.futures.as_completed(futures):
                (zoom, x, y), processed, tile_active_pixels = future.result()
                destination = stage / str(zoom) / str(x) / f"{y}.png"
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(processed)
                active_pixels += tile_active_pixels
        if target.exists():
            shutil.rmtree(target)
        os.replace(stage, target)
    finally:
        if stage.exists():
            shutil.rmtree(stage, ignore_errors=True)

    fetched_at = now().astimezone(dt.timezone.utc)
    status: dict[str, object] = {
        "schema": SCHEMA,
        "available": True,
        "analysis_only": True,
        "forecast_minutes": 0,
        "processed": True,
        "source": "JMA High-resolution Precipitation Nowcast analysis",
        "source_page": DEFAULT_SOURCE_PAGE,
        "attribution": "Precipitation: JMA, processed",
        "basetime": basetime,
        "validtime": validtime,
        "observed_at_utc": iso_z(observed_at),
        "fetched_at_utc": iso_z(fetched_at),
        "stale_after_sec": config.stale_sec,
        "fresh_until_utc": iso_z(observed_at + dt.timedelta(seconds=config.stale_sec)),
        "bounds": list(config.bounds),
        "tile_zooms": list(config.tile_zooms),
        "minzoom": min(config.tile_zooms),
        "maxzoom": max(config.tile_zooms),
        "tile_count": len(tiles),
        "active_pixel_count": active_pixels,
        "has_precipitation": active_pixels > 0,
        "tile_template": f"/weather/tiles/{validtime}/{{z}}/{{x}}/{{y}}.png",
        "processing": "JMA intensity classes recolored; values below 1 mm/h transparent",
    }
    write_json_atomic(status_path, status)
    prune_generations(generations_root, config.keep_generations)
    return status, True


def write_health(
    config: FetcherConfig,
    *,
    state: str,
    success: bool | None,
    detail: str,
    now: dt.datetime,
    consecutive_failures: int = 0,
    last_success_at: dt.datetime | None = None,
    next_retry_at: dt.datetime | None = None,
) -> None:
    payload: dict[str, object] = {
        "schema": "stream_v3.precipitation_fetcher_health.v1",
        "checked_at_utc": iso_z(now),
        "state": state,
        "success": success,
        "detail": detail[:240],
        "consecutive_failures": max(0, consecutive_failures),
        "last_success_at_utc": iso_z(last_success_at) if last_success_at else None,
        "next_retry_at_utc": iso_z(next_retry_at) if next_retry_at else None,
    }
    write_json_atomic(config.output_root / "health.json", payload)


def normal_poll_delay(now: dt.datetime, *, poll_sec: int, publish_delay_sec: int) -> float:
    period = max(5, poll_sec)
    offset = min(max(0, publish_delay_sec), period - 1)
    epoch = now.astimezone(dt.timezone.utc).timestamp()
    next_epoch = math.floor(epoch / period) * period + offset
    if next_epoch <= epoch:
        next_epoch += period
    return max(5.0, next_epoch - epoch)


def failure_retry_delay(config: FetcherConfig, consecutive_failures: int, now: dt.datetime) -> float:
    index = consecutive_failures - 1
    if 0 <= index < len(config.retry_delays_sec):
        return float(config.retry_delays_sec[index])
    return normal_poll_delay(
        now,
        poll_sec=config.poll_sec,
        publish_delay_sec=config.publish_delay_sec,
    )


def run_loop(
    config: FetcherConfig,
    *,
    refresh: Callable[[FetcherConfig], tuple[dict[str, object], bool]] = refresh_once,
    now: Callable[[], dt.datetime] = utc_now,
    sleep: Callable[[float], None] = time.sleep,
    max_cycles: int | None = None,
) -> None:
    previous_health = read_status(config.output_root / "health.json") or {}
    previous_status = read_status(config.output_root / "status.json") or {}
    last_success_text = str(
        previous_health.get("last_success_at_utc")
        or previous_status.get("fetched_at_utc")
        or ""
    )
    try:
        last_success_at = dt.datetime.fromisoformat(last_success_text.replace("Z", "+00:00"))
    except ValueError:
        last_success_at = None
    started_at = now()
    write_health(
        config,
        state="warming_up",
        success=None,
        detail="initial precipitation request pending",
        now=started_at,
        last_success_at=last_success_at,
        next_retry_at=started_at,
    )
    consecutive_failures = 0
    cycles = 0
    while True:
        try:
            status, changed = refresh(config)
            checked_at = now()
            consecutive_failures = 0
            last_success_at = checked_at
            delay = normal_poll_delay(
                checked_at,
                poll_sec=config.poll_sec,
                publish_delay_sec=config.publish_delay_sec,
            )
            detail = f"validtime={status.get('validtime')} changed={str(changed).lower()}"
            write_health(
                config,
                state="current",
                success=True,
                detail=detail,
                now=checked_at,
                last_success_at=last_success_at,
                next_retry_at=checked_at + dt.timedelta(seconds=delay),
            )
            print(f"[precipitation] {detail}", flush=True)
        except Exception as exc:  # Weather failure must not terminate the sidecar loop.
            checked_at = now()
            consecutive_failures += 1
            delay = failure_retry_delay(config, consecutive_failures, checked_at)
            detail = f"{type(exc).__name__}: {exc}"
            try:
                write_health(
                    config,
                    state="retrying",
                    success=False,
                    detail=detail,
                    now=checked_at,
                    consecutive_failures=consecutive_failures,
                    last_success_at=last_success_at,
                    next_retry_at=checked_at + dt.timedelta(seconds=delay),
                )
            except Exception:
                pass
            print(f"[precipitation] refresh failed: {detail}", flush=True)
        cycles += 1
        if max_cycles is not None and cycles >= max_cycles:
            return
        sleep(delay)


def parse_bounds(value: str) -> tuple[float, float, float, float]:
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("bounds must be west,south,east,north")
    try:
        bounds = tuple(float(part) for part in parts)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("bounds must contain numeric values") from exc
    tiles_for_bounds(bounds, (0,))
    return bounds  # type: ignore[return-value]


def parse_zooms(value: str) -> tuple[int, ...]:
    try:
        zooms = tuple(sorted({int(part.strip()) for part in value.split(",") if part.strip()}))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("tile zooms must be comma-separated integers") from exc
    if not zooms or any(zoom < 4 or zoom > 10 or zoom % 2 for zoom in zooms):
        raise argparse.ArgumentTypeError("JMA precipitation tile zooms must be even values from 4 to 10")
    return zooms


def parse_retry_delays(value: str) -> tuple[int, ...]:
    try:
        delays = tuple(int(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("retry delays must be comma-separated seconds") from exc
    if not delays or any(delay < 5 for delay in delays):
        raise argparse.ArgumentTypeError("retry delays must contain values of at least 5 seconds")
    return delays


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"", "0", "false", "no", "off"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="fetch one generation and exit")
    mode.add_argument("--loop", action="store_true", help="continue polling")
    parser.add_argument(
        "--output-root",
        default=os.environ.get("PRECIPITATION_ROOT", "/state/overlay/precipitation"),
    )
    parser.add_argument(
        "--metadata-url",
        default=os.environ.get("JMA_NOWC_METADATA_URL", DEFAULT_METADATA_URL),
    )
    parser.add_argument(
        "--data-root-url",
        default=os.environ.get("JMA_NOWC_DATA_ROOT_URL", DEFAULT_DATA_ROOT_URL),
    )
    parser.add_argument(
        "--bounds",
        type=parse_bounds,
        default=parse_bounds(os.environ.get("PRECIPITATION_BOUNDS", ",".join(map(str, DEFAULT_BOUNDS)))),
    )
    parser.add_argument(
        "--tile-zooms",
        type=parse_zooms,
        default=parse_zooms(os.environ.get("PRECIPITATION_TILE_ZOOMS", "6")),
    )
    parser.add_argument(
        "--poll-sec",
        type=int,
        default=int(os.environ.get("PRECIPITATION_POLL_SEC", "300")),
    )
    parser.add_argument(
        "--publish-delay-sec",
        type=int,
        default=int(os.environ.get("PRECIPITATION_PUBLISH_DELAY_SEC", "25")),
    )
    parser.add_argument(
        "--retry-delays-sec",
        type=parse_retry_delays,
        default=parse_retry_delays(os.environ.get("PRECIPITATION_RETRY_DELAYS_SEC", "15,45,120")),
    )
    parser.add_argument(
        "--stale-sec",
        type=int,
        default=int(os.environ.get("PRECIPITATION_STALE_SEC", "900")),
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=float(os.environ.get("PRECIPITATION_TIMEOUT_SEC", "20")),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.environ.get("PRECIPITATION_WORKERS", "4")),
    )
    parser.add_argument(
        "--keep-generations",
        type=int,
        default=int(os.environ.get("PRECIPITATION_KEEP_GENERATIONS", "3")),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not env_bool("PRECIPITATION_ENABLED", True):
        print("[precipitation] disabled", flush=True)
        if args.loop:
            while True:
                time.sleep(3600)
        return 0
    config = FetcherConfig(
        output_root=Path(args.output_root).resolve(),
        metadata_url=args.metadata_url,
        data_root_url=args.data_root_url,
        bounds=args.bounds,
        tile_zooms=args.tile_zooms,
        poll_sec=max(5, args.poll_sec),
        publish_delay_sec=max(0, args.publish_delay_sec),
        retry_delays_sec=args.retry_delays_sec,
        stale_sec=max(60, args.stale_sec),
        timeout_sec=max(1.0, args.timeout_sec),
        workers=max(1, args.workers),
        keep_generations=max(1, args.keep_generations),
    )
    if args.loop:
        run_loop(config)
        return 0
    status, changed = refresh_once(config)
    checked_at = utc_now()
    write_health(
        config,
        state="current",
        success=True,
        detail=f"validtime={status.get('validtime')} changed={str(changed).lower()}",
        now=checked_at,
        last_success_at=checked_at,
    )
    print(json.dumps(status, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
