from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import quote


RunCommand = Callable[[list[str]], subprocess.CompletedProcess[str]]


def display_ready(display_name: str, *, run_cmd: RunCommand) -> bool:
    return run_cmd(["xdpyinfo", "-display", display_name]).returncode == 0


def start_x_display(cfg, *, run_cmd: RunCommand) -> subprocess.Popen | None:
    if display_ready(cfg.display_name, run_cmd=run_cmd):
        return None
    if not cfg.auto_start_xvfb:
        raise RuntimeError(f"X display is unavailable: {cfg.display_name}")
    if shutil.which("Xvfb") is None:
        raise RuntimeError("Xvfb not found.")
    cfg.xvfb_log_file.parent.mkdir(parents=True, exist_ok=True)
    with cfg.xvfb_log_file.open("a", encoding="utf-8") as lf:
        proc = subprocess.Popen(
            ["Xvfb", cfg.display_name, "-screen", "0", f"{cfg.video_size}x{cfg.xvfb_depth}", "-ac", "-nolisten", "tcp"],
            stdout=lf,
            stderr=lf,
        )
    for _ in range(40):
        if display_ready(cfg.display_name, run_cmd=run_cmd):
            return proc
        time.sleep(0.25)
    raise RuntimeError(f"Failed to start X display: {cfg.display_name}")


def is_port_listening(host: str, port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.5)
    try:
        return sock.connect_ex((host, port)) == 0
    finally:
        sock.close()


def http_get_text(url: str, timeout_sec: float = 2.0) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "stream-new-ready-probe/1.0",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as r:
        return r.read().decode("utf-8", errors="ignore")


def overlay_http_ready_probe(cfg) -> tuple[bool, str]:
    if not cfg.use_overlay_wrapper:
        return True, "overlay wrapper disabled"
    if not is_port_listening("127.0.0.1", cfg.overlay_port):
        return False, f"overlay port not listening: {cfg.overlay_port}"

    base = f"http://{cfg.overlay_view_host}:{cfg.overlay_port}"
    try:
        html = http_get_text(f"{base}/index.html", timeout_sec=2.0)
    except urllib.error.URLError as e:
        return False, f"overlay index fetch failed: {e}"
    except Exception as e:
        return False, f"overlay index fetch failed: {e}"

    markers = ('id="map"', "Local ADS-B Receiver", "Evaluated with ARENA")
    if not all(marker in html for marker in markers):
        return False, "overlay index missing expected markers"

    try:
        map_html = http_get_text(f"{base}/stream1090/", timeout_sec=2.0)
    except urllib.error.URLError as e:
        return False, f"stream1090 fetch failed: {e}"
    except Exception as e:
        return False, f"stream1090 fetch failed: {e}"
    if len(map_html.strip()) < 64:
        return False, "stream1090 response too short"
    return True, "overlay and stream1090 reachable"


def start_overlay_server(cfg) -> subprocess.Popen | None:
    if not cfg.use_overlay_wrapper:
        return None
    if is_port_listening("127.0.0.1", cfg.overlay_port):
        return None
    cfg.overlay_server_log_file.parent.mkdir(parents=True, exist_ok=True)
    with cfg.overlay_server_log_file.open("a", encoding="utf-8") as lf:
        proc = subprocess.Popen(
            [
                sys.executable,
                str(cfg.base_dir / "src" / "stream_core" / "overlay_server.py"),
                "--port",
                str(cfg.overlay_port),
                "--host",
                cfg.overlay_bind_host,
                "--directory",
                str(cfg.overlay_dir),
                "--stream1090-url",
                cfg.stream1090_url,
            ],
            stdout=lf,
            stderr=lf,
        )
    time.sleep(1.0)
    return proc


def build_browser_url(cfg) -> str:
    if not cfg.use_overlay_wrapper:
        return cfg.browser_url
    base = quote(f"http://{cfg.overlay_view_host}:{cfg.overlay_port}/stream1090/", safe=":/?=%#-_.~")
    return (
        f"http://{cfg.overlay_view_host}:{cfg.overlay_port}/index.html"
        f"?map_base={base}&lat={cfg.map_lat}&lon={cfg.map_lon}&zoom={cfg.map_zoom}"
        f"&scale={cfg.map_scale}&iconScale={cfg.map_icon_scale}&labelScale={cfg.map_label_scale}"
        f"&largeMode={cfg.map_large_mode}"
    )


def resolve_browser_bin(browser_bin: str) -> Optional[str]:
    if browser_bin:
        return browser_bin if shutil.which(browser_bin) else None
    for candidate in ("chromium-browser", "chromium", "google-chrome", "google-chrome-stable", "firefox"):
        if shutil.which(candidate):
            return candidate
    return None


def start_browser(cfg, *, settle_sec: float, url: str) -> subprocess.Popen | None:
    if not cfg.auto_start_browser:
        return None
    browser = resolve_browser_bin(cfg.browser_bin)
    if not browser:
        raise RuntimeError("Browser not found for URL rendering.")
    if cfg.reset_browser_profile and cfg.browser_profile_dir.exists():
        shutil.rmtree(cfg.browser_profile_dir, ignore_errors=True)
    cfg.browser_profile_dir.mkdir(parents=True, exist_ok=True)
    window_size = cfg.browser_window_size or f"{cfg.video_size.split('x')[0]},{cfg.video_size.split('x')[1]}"
    env = os.environ.copy()
    env["DISPLAY"] = cfg.display_name
    cfg.browser_log_file.parent.mkdir(parents=True, exist_ok=True)
    with cfg.browser_log_file.open("a", encoding="utf-8") as lf:
        if browser == "firefox":
            args = [browser, "--kiosk", url]
        else:
            args = [
                browser,
                f"--app={url}",
                "--kiosk",
                "--lang=en-US",
                "--autoplay-policy=no-user-gesture-required",
                "--blink-settings=translateEnabled=false",
                "--no-first-run",
                "--no-default-browser-check",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-translate",
                "--disable-features=Translate,TranslateUI,LanguageDetection",
                "--disable-extensions",
                "--disable-component-extensions-with-background-pages",
                "--disable-session-crashed-bubble",
                "--disable-infobars",
                "--force-device-scale-factor=1",
                "--high-dpi-support=1",
                f"--user-data-dir={cfg.browser_profile_dir}",
                f"--window-size={window_size}",
                f"--window-position={cfg.browser_window_pos}",
            ]
        proc = subprocess.Popen(args, stdout=lf, stderr=lf, env=env)
    if settle_sec > 0:
        time.sleep(settle_sec)
    return proc
