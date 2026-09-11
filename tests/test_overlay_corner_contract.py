from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OVERLAY_HTML = ROOT / "ui" / "overlay" / "index.html"
MAP_HTML = ROOT / "ui" / "overlay" / "adsb-map" / "index.html"


def overlay_html() -> str:
    return OVERLAY_HTML.read_text(encoding="utf-8")


class OverlayCornerContractTests(unittest.TestCase):
    def test_three_information_panels_are_present(self) -> None:
        html = overlay_html()
        self.assertIn('id="adsb"', html)
        self.assertIn('id="np"', html)
        self.assertIn('id="info"', html)
        self.assertNotIn('id="arenaFooter"', html)
        self.assertIn("Local ADS-B Receiver", html)
        self.assertIn("Background Music", html)
        self.assertIn("JST", html)

    def test_corner_positions_match_stream_layout(self) -> None:
        html = overlay_html()
        self.assertRegex(html, re.compile(r"#adsb\s*\{[^}]*top:\s*12px;[^}]*left:\s*12px;", re.S))
        self.assertRegex(html, re.compile(r"#np\s*\{[^}]*top:\s*18px;[^}]*right:\s*18px;", re.S))
        self.assertRegex(html, re.compile(r"#info\s*\{[^}]*left:\s*0;[^}]*bottom:\s*0;", re.S))

    def test_duplicate_right_footer_is_replaced_by_map_attribution(self) -> None:
        overlay = overlay_html()
        map_html = MAP_HTML.read_text(encoding="utf-8")

        self.assertNotIn("arenaFooter", overlay)
        self.assertIn("ADS-B: Local receiver · Evaluated with ARENA · Not for navigation", map_html)
        self.assertIn("Map: OpenFreeMap", map_html)
        self.assertIn("Precipitation: JMA, processed", map_html)

    def test_left_panel_targets_are_aircraft_json_count(self) -> None:
        html = overlay_html()
        self.assertIn('const aircraft = Array.isArray(data.aircraft) ? data.aircraft : [];', html)
        self.assertIn('setText("aircraftCount", String(aircraft.length));', html)
        self.assertIn('setText("positionCount", String(positions));', html)
        self.assertIn('setText("receiverStatus", stale ? "STALE" : "OK");', html)

    def test_left_panel_is_one_integrated_instrument_card(self) -> None:
        html = overlay_html()

        adsb = html.split('<aside id="adsb"', 1)[1].split("</aside>", 1)[0]
        self.assertIn("width: min(460px", html)
        self.assertIn("#adsb .kicker", html)
        self.assertIn("text-align: center", html.split("#adsb .kicker", 1)[1].split("}", 1)[0])
        self.assertIn("receiverTitleAlignment: getComputedStyle(receiverTitle).textAlign", html)
        self.assertEqual(adsb.count('class="instrument-section'), 3)
        self.assertIn("Local ADS-B Receiver", adsb)
        self.assertEqual(adsb.count('class="altitude-row"'), 2)
        self.assertIn("24H LOS-AWARE COVERAGE", adsb)
        self.assertIn("Farthest now", adsb)
        self.assertIn("--coverage: #A29BBA", html)
        self.assertNotIn("#82969D", html)
        self.assertIn("PRECIPITATION", adsb)
        self.assertIn('id="precipitationState"', adsb)
        self.assertIn('u.searchParams.set("embedded", "1");', html)

    def test_primary_overlay_text_is_large_enough_for_mobile_landscape_scaling(self) -> None:
        html = overlay_html()

        self.assertIn("width: min(460px", html)
        self.assertRegex(html, re.compile(r"\.metric strong\s*\{[^}]*font-size:\s*24px;", re.S))
        self.assertRegex(html, re.compile(r"#np \.title\s*\{[^}]*font-size:\s*26px;", re.S))
        self.assertRegex(html, re.compile(r"\.time-row strong\s*\{[^}]*font-size:\s*17px;", re.S))

    def test_phone_layout_compacts_panels_without_overlapping_fixed_offsets(self) -> None:
        html = overlay_html()
        phone = html.split("@media (max-width: 480px)", 1)[1].split("</style>", 1)[0]

        self.assertIn("#adsb .altitude-section { display: none; }", phone)
        self.assertRegex(phone, re.compile(r"#adsb\s*\{\s*top:\s*8px;\s*\}"))
        self.assertRegex(phone, re.compile(r"#np\s*\{[^}]*top:\s*230px;", re.S))
        self.assertIn("grid-template-columns: repeat(4, minmax(0, 1fr))", phone)
        self.assertIn("#info .time-row span:last-child { display: none; }", phone)
        self.assertIn("width: calc(100vw - 16px)", phone)

    def test_precipitation_card_reports_warmup_retry_lkg_and_unavailable_states(self) -> None:
        html = overlay_html()

        self.assertIn("Promise.allSettled([", html)
        self.assertIn('fetch(`./weather/status.json?${cacheBust}`', html)
        self.assertIn('fetch(`./weather/health.json?${cacheBust}`', html)
        for state in ("WARMING UP", "CURRENT", "NO RAIN", "LAST DATA", "STALE", "DATA UNAVAILABLE"):
            self.assertIn(f'state: "{state}"', html)
        self.assertIn('retryNotice || "INITIAL REQUEST"', html)
        self.assertIn('retryNotice || "RETRYING"', html)
        self.assertIn("PRECIPITATION_WARMUP_MS", html)
        self.assertIn("PRECIPITATION_UNAVAILABLE_MS", html)
        self.assertIn('health?.state === "warming_up"', html)
        self.assertIn("nowMs - healthCheckedMs <= PRECIPITATION_WARMUP_MS", html)
        self.assertIn("setTimeout(refreshPrecipitation, PRECIPITATION_WARMUP_MS + 250);", html)
        self.assertIn('id="precipitationNotice"', html)
        self.assertNotIn('setPrecipitationState("OFFLINE")', html)
        self.assertIn("setInterval(refreshPrecipitation, PRECIPITATION_REFRESH_MS);", html)
        self.assertIn("validatedPrecipitationPayloads(statusCandidate, healthCandidate)", html)
        self.assertIn("lastGoodPrecipitationStatus", html)
        self.assertIn('state: "retrying", success: false', html)

    def test_right_panel_title_uses_now_playing(self) -> None:
        html = overlay_html()
        self.assertIn("fetchNowPlayingJson()", html)
        self.assertIn("fetchNowPlayingText()", html)
        self.assertIn("const jsonTitle = cleanNowPlayingTitle(np.title || np.title_line);", html)
        self.assertIn("validNowPlayingSnapshot(payload)", html)
        self.assertIn("validNowPlayingText(text)", html)
        self.assertIn("const title = mockTitle", html)
        self.assertIn("? cleanNowPlayingTitle(mockTitle)", html)
        self.assertIn(': textTitle || jsonTitle || currentNowPlayingTitle || "Unknown title";', html)
        self.assertIn("titleEl.textContent = title;", html)
        self.assertIn("repairMojibakeTitle(String(value || \"\"))", html)

    def test_overlay_uses_installed_unicode_font_family_names(self) -> None:
        html = overlay_html()

        self.assertIn('"Noto Sans CJK JP"', html)
        self.assertIn('"Noto Color Emoji"', html)
        self.assertNotIn('"Noto Sans JP"', html)

    def test_right_panel_credit_tracks_music_provider(self) -> None:
        html = overlay_html()

        self.assertIn('id="musicProviderBadge"', html)
        self.assertIn('id="musicCredit"', html)
        self.assertIn('badge: "FLORACORE · EVENING"', html)
        self.assertIn('credit: "Music provided by @Floracore_EDM"', html)
        self.assertIn('credit: "Music provided by NoCopyrightSounds"', html)
        self.assertIn("function musicProviderFor(nowPlaying, title, override = \"\")", html)
        self.assertIn('const mockMusicSource = params.get("mockMusicSource");', html)
        self.assertIn("applyMusicProvider(musicProvider);", html)
        self.assertIn('panel.dataset.musicProvider = provider.id;', html)

    def test_overlay_parent_refresh_cadence_keeps_now_playing_fast(self) -> None:
        html = overlay_html()
        self.assertIn('const ADSB_REFRESH_MS = intervalParam("adsbRefreshMs", 5_000, 1_000);', html)
        self.assertIn('const NOW_PLAYING_REFRESH_MS = intervalParam("nowPlayingRefreshMs", 1_000, 1_000);', html)
        self.assertIn('const MAP_RELOAD_MIN_MS = intervalParam("mapReloadMinMs", 120_000, 30_000);', html)
        self.assertIn("setInterval(refreshAdsb, ADSB_REFRESH_MS);", html)
        self.assertIn("setInterval(refresh, NOW_PLAYING_REFRESH_MS);", html)
        self.assertNotIn("setInterval(refreshAdsb, 1000);", html)

    def test_overlay_avoids_rewriting_unchanged_text(self) -> None:
        html = overlay_html()
        self.assertIn("if (el && el.textContent !== value) el.textContent = value;", html)
        self.assertIn("if (title !== currentNowPlayingTitle)", html)
        self.assertIn("if (stale !== currentNowPlayingStale)", html)


if __name__ == "__main__":
    unittest.main()
