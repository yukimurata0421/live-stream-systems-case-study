from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core import precipitation_fetcher as fetcher


def palette_tile(indices: list[int], size: tuple[int, int] | None = None) -> bytes:
    image = Image.new("P", size or (len(indices), 1))
    palette = [0] * 768
    colors = {
        0: (255, 255, 255),
        2: (242, 242, 255),
        3: (160, 210, 255),
        4: (33, 140, 255),
        5: (0, 65, 255),
        6: (250, 245, 0),
        7: (255, 153, 0),
        8: (255, 40, 0),
        9: (180, 0, 104),
    }
    for index, color in colors.items():
        palette[index * 3 : index * 3 + 3] = color
    image.putpalette(palette)
    image.putdata(indices)
    encoded = io.BytesIO()
    image.save(encoded, format="PNG", bits=4, transparency=bytes([0] + [255] * 255))
    return encoded.getvalue()


def rgba_pixels(encoded: bytes) -> list[tuple[int, int, int, int]]:
    image = Image.open(io.BytesIO(encoded)).convert("RGBA")
    pixels = image.get_flattened_data() if hasattr(image, "get_flattened_data") else image.getdata()
    return list(pixels)


class PrecipitationFetcherTests(unittest.TestCase):
    def test_retry_delays_and_aligned_normal_poll_are_deterministic(self) -> None:
        self.assertEqual(fetcher.parse_retry_delays("15,45,120"), (15, 45, 120))
        with self.assertRaisesRegex(argparse.ArgumentTypeError, "at least 5 seconds"):
            fetcher.parse_retry_delays("2,10")

        now = dt.datetime(2026, 8, 2, 11, 20, 30, tzinfo=dt.timezone.utc)
        self.assertEqual(fetcher.normal_poll_delay(now, poll_sec=300, publish_delay_sec=25), 295.0)
        config = fetcher.FetcherConfig(
            output_root=Path("/tmp/unused-precipitation-test"),
            metadata_url="https://example.test/metadata.json",
            data_root_url="https://example.test/tiles",
            bounds=fetcher.DEFAULT_BOUNDS,
            tile_zooms=(6,),
            retry_delays_sec=(15, 45, 120),
        )
        self.assertEqual(fetcher.failure_retry_delay(config, 1, now), 15.0)
        self.assertEqual(fetcher.failure_retry_delay(config, 2, now), 45.0)
        self.assertEqual(fetcher.failure_retry_delay(config, 3, now), 120.0)
        self.assertEqual(fetcher.failure_retry_delay(config, 4, now), 295.0)

    def test_tile_zooms_reject_jma_odd_zoom_levels(self) -> None:
        self.assertEqual(fetcher.parse_zooms("4,6,10"), (4, 6, 10))
        with self.assertRaisesRegex(argparse.ArgumentTypeError, "even values"):
            fetcher.parse_zooms("7")

    def test_latest_hrpns_analysis_excludes_forecast_frames(self) -> None:
        selected = fetcher.select_latest_analysis(
            [
                {
                    "basetime": "20260802080000",
                    "validtime": "20260802080500",
                    "elements": ["hrpns"],
                },
                {
                    "basetime": "20260802075500",
                    "validtime": "20260802075500",
                    "elements": ["hrpns"],
                },
                {
                    "basetime": "20260802080000",
                    "validtime": "20260802080000",
                    "elements": ["hrpns", "hrpns_nd"],
                },
            ]
        )

        self.assertEqual(selected["validtime"], "20260802080000")
        self.assertEqual(selected["basetime"], selected["validtime"])

    def test_fixed_stream_bounds_use_bounded_even_zoom_tiles(self) -> None:
        tiles = fetcher.tiles_for_bounds(fetcher.DEFAULT_BOUNDS, fetcher.DEFAULT_TILE_ZOOMS)

        self.assertEqual(len(tiles), 8)
        self.assertEqual({tile[0] for tile in tiles}, {6})
        self.assertIn((6, 57, 25), tiles)

    def test_palette_processing_hides_below_one_and_uses_intensity_alpha(self) -> None:
        processed, active_pixels = fetcher.process_precipitation_tile(palette_tile([2, 3, 4, 5, 6, 9]))
        pixels = rgba_pixels(processed)

        self.assertEqual(active_pixels, 5)
        self.assertEqual([pixel[3] for pixel in pixels], [0, 26, 56, 71, 97, 153])
        self.assertEqual(pixels[1][:3], (116, 145, 158))
        self.assertEqual(pixels[-1][:3], (157, 54, 55))

    def test_rgba_processing_keeps_empty_pixels_transparent(self) -> None:
        image = Image.new("RGBA", (3, 1))
        image.putdata([(0, 0, 0, 0), (160, 210, 255, 255), (255, 40, 0, 255)])
        encoded = io.BytesIO()
        image.save(encoded, format="PNG")

        processed, active_pixels = fetcher.process_precipitation_tile(encoded.getvalue())
        pixels = rgba_pixels(processed)

        self.assertEqual(active_pixels, 2)
        self.assertEqual(pixels[0][3], 0)
        self.assertEqual(pixels[1], (116, 145, 158, 26))
        self.assertEqual(pixels[2], (179, 76, 64, 140))

    def test_refresh_publishes_local_analysis_generation_atomically(self) -> None:
        metadata_url = "https://www.jma.go.jp/example-target-times.json"
        metadata = json.dumps(
            [
                {
                    "basetime": "20260802080000",
                    "validtime": "20260802080500",
                    "elements": ["hrpns"],
                },
                {
                    "basetime": "20260802080000",
                    "validtime": "20260802080000",
                    "elements": ["hrpns", "hrpns_nd"],
                },
            ]
        ).encode("utf-8")
        tile = palette_tile([3] * (256 * 256), (256, 256))
        calls: list[str] = []

        def fake_fetch(url: str, _timeout: float) -> bytes:
            calls.append(url)
            return metadata if url == metadata_url else tile

        with tempfile.TemporaryDirectory() as td:
            config = fetcher.FetcherConfig(
                output_root=Path(td),
                metadata_url=metadata_url,
                data_root_url="https://www.jma.go.jp/tiles",
                bounds=(140.0, 35.0, 141.0, 36.0),
                tile_zooms=(6,),
                stale_sec=900,
            )
            fixed_now = lambda: dt.datetime(2026, 8, 2, 8, 1, tzinfo=dt.timezone.utc)

            status, changed = fetcher.refresh_once(config, fetch=fake_fetch, now=fixed_now)
            status_again, changed_again = fetcher.refresh_once(config, fetch=fake_fetch, now=fixed_now)

            self.assertTrue(changed)
            self.assertFalse(changed_again)
            self.assertTrue(status["analysis_only"])
            self.assertEqual(status["forecast_minutes"], 0)
            self.assertTrue(status["has_precipitation"])
            self.assertEqual(status["tile_template"], "/weather/tiles/20260802080000/{z}/{x}/{y}.png")
            self.assertEqual(status_again["validtime"], "20260802080000")
            self.assertTrue((Path(td) / "status.json").is_file())
            generation = Path(td) / "generations" / "20260802080000"
            self.assertEqual(len(list(generation.rglob("*.png"))), status["tile_count"])
            tile_calls = [url for url in calls if url != metadata_url]
            self.assertEqual(len(tile_calls), status["tile_count"])

    def test_failed_new_generation_preserves_last_known_good_status_and_tiles(self) -> None:
        metadata_url = "https://www.jma.go.jp/example-target-times.json"
        selected_validtime = "20260802080000"
        tile = palette_tile([3] * (256 * 256), (256, 256))

        def metadata(validtime: str) -> bytes:
            return json.dumps(
                [{"basetime": validtime, "validtime": validtime, "elements": ["hrpns"]}]
            ).encode("utf-8")

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = fetcher.FetcherConfig(
                output_root=root,
                metadata_url=metadata_url,
                data_root_url="https://www.jma.go.jp/tiles",
                bounds=(140.0, 35.0, 141.0, 36.0),
                tile_zooms=(6,),
                stale_sec=900,
            )

            def initial_fetch(url: str, _timeout: float) -> bytes:
                return metadata(selected_validtime) if url == metadata_url else tile

            original_status, _changed = fetcher.refresh_once(config, fetch=initial_fetch)
            original_status_bytes = (root / "status.json").read_bytes()
            original_tiles = sorted(
                path.relative_to(root).as_posix()
                for path in (root / "generations" / selected_validtime).rglob("*.png")
            )

            def failing_fetch(url: str, _timeout: float) -> bytes:
                if url == metadata_url:
                    return metadata("20260802080500")
                raise TimeoutError("simulated tile timeout")

            with self.assertRaisesRegex(TimeoutError, "simulated tile timeout"):
                fetcher.refresh_once(config, fetch=failing_fetch)

            self.assertEqual((root / "status.json").read_bytes(), original_status_bytes)
            self.assertEqual(fetcher.read_status(root / "status.json"), original_status)
            self.assertEqual(
                sorted(
                    path.relative_to(root).as_posix()
                    for path in (root / "generations" / selected_validtime).rglob("*.png")
                ),
                original_tiles,
            )
            self.assertFalse((root / "generations" / "20260802080500").exists())

    def test_loop_retries_quickly_and_preserves_previous_success_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            previous_success = "2026-08-02T07:55:25Z"
            (root / "status.json").write_text(
                json.dumps({"fetched_at_utc": previous_success}) + "\n",
                encoding="utf-8",
            )
            config = fetcher.FetcherConfig(
                output_root=root,
                metadata_url="https://example.test/metadata.json",
                data_root_url="https://example.test/tiles",
                bounds=fetcher.DEFAULT_BOUNDS,
                tile_zooms=(6,),
                retry_delays_sec=(15, 45, 120),
            )
            times = iter(
                [
                    dt.datetime(2026, 8, 2, 8, 0, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2026, 8, 2, 8, 0, 1, tzinfo=dt.timezone.utc),
                    dt.datetime(2026, 8, 2, 8, 0, 16, tzinfo=dt.timezone.utc),
                    dt.datetime(2026, 8, 2, 8, 1, 1, tzinfo=dt.timezone.utc),
                    dt.datetime(2026, 8, 2, 8, 3, 1, tzinfo=dt.timezone.utc),
                ]
            )
            sleeps: list[float] = []

            def always_fail(_config: fetcher.FetcherConfig) -> tuple[dict[str, object], bool]:
                raise TimeoutError("simulated upstream timeout")

            fetcher.run_loop(
                config,
                refresh=always_fail,
                now=lambda: next(times),
                sleep=sleeps.append,
                max_cycles=4,
            )

            health = fetcher.read_status(root / "health.json")
            self.assertIsNotNone(health)
            assert health is not None
            self.assertEqual(sleeps, [15.0, 45.0, 120.0])
            self.assertEqual(health["state"], "retrying")
            self.assertFalse(health["success"])
            self.assertEqual(health["consecutive_failures"], 4)
            self.assertEqual(health["last_success_at_utc"], previous_success)
            self.assertEqual(health["next_retry_at_utc"], "2026-08-02T08:05:25Z")

    def test_loop_recovery_resets_failure_count_and_returns_to_aligned_polling(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = fetcher.FetcherConfig(
                output_root=root,
                metadata_url="https://example.test/metadata.json",
                data_root_url="https://example.test/tiles",
                bounds=fetcher.DEFAULT_BOUNDS,
                tile_zooms=(6,),
                retry_delays_sec=(15, 45, 120),
            )
            times = iter(
                [
                    dt.datetime(2026, 8, 2, 8, 0, 0, tzinfo=dt.timezone.utc),
                    dt.datetime(2026, 8, 2, 8, 0, 1, tzinfo=dt.timezone.utc),
                    dt.datetime(2026, 8, 2, 8, 0, 16, tzinfo=dt.timezone.utc),
                    dt.datetime(2026, 8, 2, 8, 1, 1, tzinfo=dt.timezone.utc),
                ]
            )
            sleeps: list[float] = []
            attempts = 0

            def fail_twice_then_recover(
                _config: fetcher.FetcherConfig,
            ) -> tuple[dict[str, object], bool]:
                nonlocal attempts
                attempts += 1
                if attempts <= 2:
                    raise TimeoutError(f"simulated timeout {attempts}")
                return {"validtime": "20260802080000"}, True

            fetcher.run_loop(
                config,
                refresh=fail_twice_then_recover,
                now=lambda: next(times),
                sleep=sleeps.append,
                max_cycles=3,
            )

            health = fetcher.read_status(root / "health.json")
            self.assertIsNotNone(health)
            assert health is not None
            self.assertEqual(attempts, 3)
            self.assertEqual(sleeps, [15.0, 45.0])
            self.assertEqual(health["state"], "current")
            self.assertTrue(health["success"])
            self.assertEqual(health["consecutive_failures"], 0)
            self.assertEqual(health["last_success_at_utc"], "2026-08-02T08:01:01Z")
            self.assertEqual(health["next_retry_at_utc"], "2026-08-02T08:05:25Z")
            self.assertEqual(health["detail"], "validtime=20260802080000 changed=true")

    def test_refresh_publishes_no_rain_as_valid_current_data(self) -> None:
        metadata_url = "https://www.jma.go.jp/example-target-times.json"
        metadata = json.dumps(
            [{"basetime": "20260802080000", "validtime": "20260802080000", "elements": ["hrpns"]}]
        ).encode("utf-8")
        dry_tile = palette_tile([2] * (256 * 256), (256, 256))

        def fake_fetch(url: str, _timeout: float) -> bytes:
            return metadata if url == metadata_url else dry_tile

        with tempfile.TemporaryDirectory() as td:
            config = fetcher.FetcherConfig(
                output_root=Path(td),
                metadata_url=metadata_url,
                data_root_url="https://www.jma.go.jp/tiles",
                bounds=(140.0, 35.0, 141.0, 36.0),
                tile_zooms=(6,),
            )

            status, changed = fetcher.refresh_once(config, fetch=fake_fetch)

            self.assertTrue(changed)
            self.assertTrue(status["available"])
            self.assertEqual(status["active_pixel_count"], 0)
            self.assertFalse(status["has_precipitation"])
            self.assertTrue((Path(td) / "generations" / "20260802080000").is_dir())

    def test_runtime_manifest_keeps_weather_optional_from_stream_readiness(self) -> None:
        root = Path(__file__).resolve().parents[1]
        deployment = (root / "deploy/k3s/v3-runtime/deployment.yaml").read_text(encoding="utf-8")
        containerfile = (root / "deploy/k3s/Containerfile").read_text(encoding="utf-8")

        self.assertIn("name: precipitation-fetcher", deployment)
        self.assertIn("stream_core.precipitation_fetcher --loop", deployment)
        self.assertNotIn("readinessProbe", deployment.split("name: precipitation-fetcher", 1)[1])
        self.assertIn("python3-pil", containerfile)
        configmap = (root / "deploy/k3s/base/configmap-shadow.yaml").read_text(encoding="utf-8")
        self.assertIn('PRECIPITATION_PUBLISH_DELAY_SEC: "25"', configmap)
        self.assertIn("PRECIPITATION_RETRY_DELAYS_SEC: 15,45,120", configmap)
        self.assertIn('PRE_FFMPEG_OVERLAY_READY_TIMEOUT_SEC: "20"', configmap)


if __name__ == "__main__":
    unittest.main()
