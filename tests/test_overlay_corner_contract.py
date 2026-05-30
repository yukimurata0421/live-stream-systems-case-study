from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OVERLAY_HTML = ROOT / "ui" / "overlay" / "index.html"


def overlay_html() -> str:
    return OVERLAY_HTML.read_text(encoding="utf-8")


class OverlayCornerContractTests(unittest.TestCase):
    def test_four_corner_panels_are_present(self) -> None:
        html = overlay_html()
        self.assertIn('id="adsb"', html)
        self.assertIn('id="np"', html)
        self.assertIn('id="info"', html)
        self.assertIn('id="arenaFooter"', html)
        self.assertIn("Local ADS-B Receiver", html)
        self.assertIn("Background Music", html)
        self.assertIn("JST", html)
        self.assertIn("Evaluated with ARENA", html)

    def test_corner_positions_match_stream_layout(self) -> None:
        html = overlay_html()
        self.assertRegex(html, re.compile(r"#adsb\s*\{[^}]*top:\s*12px;[^}]*left:\s*12px;", re.S))
        self.assertRegex(html, re.compile(r"#np\s*\{[^}]*top:\s*18px;[^}]*right:\s*18px;", re.S))
        self.assertRegex(html, re.compile(r"#info\s*\{[^}]*left:\s*0;[^}]*bottom:\s*0;", re.S))
        self.assertRegex(html, re.compile(r"#arenaFooter\s*\{[^}]*right:\s*14px;[^}]*bottom:\s*14px;", re.S))

    def test_left_panel_targets_are_aircraft_json_count(self) -> None:
        html = overlay_html()
        self.assertIn('const aircraft = Array.isArray(data.aircraft) ? data.aircraft : [];', html)
        self.assertIn('setText("aircraftCount", String(aircraft.length));', html)
        self.assertIn('setText("positionCount", String(positions));', html)
        self.assertIn('setText("receiverStatus", stale ? "STALE" : "OK");', html)

    def test_right_panel_title_uses_now_playing(self) -> None:
        html = overlay_html()
        self.assertIn("fetchNowPlayingJson()", html)
        self.assertIn("fetchNowPlayingText()", html)
        self.assertIn("const jsonTitle = cleanNowPlayingTitle(np.title || np.title_line);", html)
        self.assertIn('const title = mockTitle || textTitle || jsonTitle || "Unknown title";', html)
        self.assertIn("titleEl.textContent = title;", html)


if __name__ == "__main__":
    unittest.main()
