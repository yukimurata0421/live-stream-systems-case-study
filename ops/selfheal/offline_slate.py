#!/usr/bin/env python3
"""Build a map-backed slate without applying config, operating services or contacting providers."""
from __future__ import annotations

import argparse
import copy
import hashlib
from http.server import ThreadingHTTPServer
import json
import mimetypes
from pathlib import Path
import shutil
import subprocess
import sys
import threading
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src'))
from stream_core.overlay_server import OverlayHandler  # noqa: E402

PAGE = Path(__file__).resolve().with_name('offline-slate')
MAP = ROOT / 'ui/overlay/adsb-map'
RUNTIME_FILE = '/opt/stream-v3-offline-slate/current/offline.mp4'
WIDTH, HEIGHT, FPS, SAMPLE_RATE = 1920, 1080, 5, 48000


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value, *, private=False):
    # Never overwrite an existing candidate, failed build or unrelated file.
    with Path(path).open('x', encoding='utf-8') as handle:
        if private:
            Path(path).chmod(0o600)
        json.dump(value, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.write('\n')


def path_patch(runtime_file=RUNTIME_FILE):
    path = Path(runtime_file)
    if not path.is_absolute() or path.suffix != '.mp4' or any(c in str(path) for c in '\r\n'):
        raise ValueError('absolute_mp4_runtime_path_required')
    return {'alwaysAvailable': True, 'alwaysAvailableFile': str(path), 'alwaysAvailableTracks': []}


def candidate_config(base, runtime_file=RUNTIME_FILE):
    result = copy.deepcopy(base)
    relay = result['paths']['relay']
    if relay.get('source', 'publisher') != 'publisher' or not isinstance(relay.get('forward'), list):
        raise ValueError('publisher_fanout_required')
    relay.update(path_patch(runtime_file))
    return result


class PreviewHandler(OverlayHandler):
    """Serve only the slate and map assets; never expose the live overlay routes."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(MAP.parent), **kwargs)

    def log_message(self, *_):
        pass

    def do_GET(self):
        path = urlsplit(self.path).path
        if path.startswith('/map-tiles/'):
            self.proxy_map_tile(urlsplit(self.path))
            return
        assets = {'/': PAGE / 'index.html', '/slate.css': PAGE / 'slate.css', '/slate.js': PAGE / 'slate.js',
                  '/map/style.json': MAP / 'style.json',
                  '/adsb-map/airports.geojson': MAP / 'airports.geojson'}
        assets.update({'/map/vendor/' + p.name: p for p in (MAP / 'vendor').iterdir() if p.is_file()})
        target = assets.get(path)
        if target is None:
            self.send_error(404)
            return
        body = target.read_bytes()
        content_type = 'text/javascript' if target.suffix == '.mjs' else mimetypes.guess_type(target.name)[0]
        self.send_response(200)
        self.send_header('Content-Type', content_type or 'application/octet-stream')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.send_error(405)

    def do_POST(self):
        self.send_error(405)


def render(output, chromium=None, *, mode='offline'):
    from playwright.sync_api import sync_playwright
    server = ThreadingHTTPServer(('127.0.0.1', 0), PreviewHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    errors = []
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True, executable_path=chromium,
                args=['--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader',
                      '--disable-dev-shm-usage', '--disable-background-networking',
                      '--renderer-process-limit=1', '--num-raster-threads=1'])
            try:
                page = browser.new_page(viewport={'width': WIDTH, 'height': HEIGHT}, device_scale_factor=1)
                page.on('pageerror', lambda error: errors.append(str(error)[:300]))
                page.add_init_script("window.addEventListener('error', () => { window.offlineSlateFailed = true; });")
                allowed = {'tiles.openfreemap.org', 'tiles.mapterhorn.com'}
                def route(request):
                    url = urlsplit(request.request.url)
                    if ((url.scheme == 'http' and url.hostname == '127.0.0.1' and url.port == server.server_port)
                            or (url.scheme == 'https' and url.hostname in allowed)):
                        request.continue_()
                    else:
                        request.abort()
                page.route('**/*', route)
                page.goto(f'http://127.0.0.1:{server.server_port}/?mode={mode}', wait_until='domcontentloaded')
                try:
                    page.wait_for_function('window.offlineSlate?.ready || window.offlineSlate?.errors.length || window.offlineSlateFailed', timeout=90000)
                    state = page.evaluate('window.offlineSlate')
                    if errors or not state or not state['ready'] or state['errors']:
                        raise ValueError('slate_render_failed')
                    page.screenshot(path=str(output), animations='disabled')
                    return state
                except Exception:
                    page.screenshot(path=str(output.with_name('failed-preview.png')), animations='disabled')
                    write_json(output.with_name('render-failure.json'), {
                        'errors': errors, 'state': page.evaluate('window.offlineSlate || null')})
                    raise
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def encode(png, target):
    subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-n', '-threads', '1', '-filter_threads', '1',
        '-loop', '1', '-framerate', str(FPS), '-i', str(png), '-f', 'lavfi', '-i', 'anullsrc=r=48000:cl=stereo',
        '-map', '0:v:0', '-map', '1:a:0', '-t', '4', '-c:v', 'libx264', '-preset', 'veryfast',
        '-threads:v', '1', '-profile:v', 'high', '-pix_fmt', 'yuv420p', '-r', str(FPS), '-g', '10',
        '-keyint_min', '10', '-sc_threshold', '0', '-bf', '0', '-crf', '20',
        '-c:a', 'aac', '-b:a', '192k', '-ar', str(SAMPLE_RATE), '-ac', '2',
        '-movflags', '+faststart', str(target)], check=True, timeout=90, capture_output=True)


def inspect_media(path):
    result = subprocess.run(['ffprobe', '-v', 'error', '-show_streams', '-show_format', '-of', 'json', str(path)],
                            check=True, capture_output=True, timeout=10)
    data = json.loads(result.stdout)
    streams = data['streams']
    if len(streams) != 2:
        raise ValueError('two_tracks_required')
    video, audio = streams
    if (video.get('codec_name') != 'h264' or (video.get('width'), video.get('height')) != (WIDTH, HEIGHT)
            or video.get('pix_fmt') != 'yuv420p' or video.get('r_frame_rate') != '5/1'
            or audio.get('codec_name') != 'aac' or audio.get('sample_rate') != str(SAMPLE_RATE)
            or audio.get('channels') != 2 or not 3.9 <= float(data['format']['duration']) <= 4.2):
        raise ValueError('slate_media_contract_mismatch')
    return {'video': 'H264', 'audio': 'AAC', 'width': WIDTH, 'height': HEIGHT, 'fps': FPS,
            'sample_rate': SAMPLE_RATE, 'channels': 2, 'duration_seconds': float(data['format']['duration'])}


def build(output, chromium=None, *, mode='offline'):
    if mode not in ('offline', 'maintenance'):
        raise ValueError('unknown_slate_mode')
    output.mkdir(parents=True, exist_ok=False)
    state = render(output / 'preview.png', chromium, mode=mode)
    encode(output / 'preview.png', output / 'offline.mp4')
    media = inspect_media(output / 'offline.mp4')
    write_json(output / 'path-patch.json', path_patch())
    shutil.copyfile(MAP / 'ATTRIBUTION.md', output / 'MAP-ATTRIBUTION.md')
    write_json(output / 'manifest.json', {'schema': 'stream_v3.offline_slate_bundle.v1',
        'content_state': 'MAINTENANCE_SLATE' if mode == 'maintenance' else 'OFFLINE_SLATE',
        'purpose': mode, 'language': 'en',
        'message': 'Scheduled maintenance' if mode == 'maintenance' else 'No data received',
        'render': state, 'media': media, 'deployed': False,
        'sources': {str(p.relative_to(ROOT)): digest(p) for p in
                    [PAGE / 'index.html', PAGE / 'slate.css', PAGE / 'slate.js', MAP / 'style.json']},
        'files': {name: digest(output / name) for name in
                  ['preview.png', 'offline.mp4', 'path-patch.json', 'MAP-ATTRIBUTION.md']}})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    build_parser = commands.add_parser('build')
    build_parser.add_argument('--output', type=Path, required=True)
    build_parser.add_argument('--chromium')
    build_parser.add_argument('--mode', choices=('offline', 'maintenance'), default='offline')
    prepare = commands.add_parser('prepare-config')
    prepare.add_argument('--base', type=Path, required=True, help='private JSON MediaMTX config')
    prepare.add_argument('--bundle', type=Path, required=True)
    prepare.add_argument('--output', type=Path, required=True)
    prepare.add_argument('--runtime-file', default=RUNTIME_FILE)
    args = parser.parse_args()
    if args.command == 'build':
        build(args.output, args.chromium, mode=args.mode)
    else:
        manifest = json.loads((args.bundle / 'manifest.json').read_text())
        if (manifest['schema'] != 'stream_v3.offline_slate_bundle.v1'
                or manifest['files']['offline.mp4'] != digest(args.bundle / 'offline.mp4')):
            raise ValueError('bundle_media_hash_mismatch')
        inspect_media(args.bundle / 'offline.mp4')
        write_json(args.output, candidate_config(json.loads(args.base.read_text()), args.runtime_file), private=True)
    print(json.dumps({'result': 'prepared', 'deployed': False}))


if __name__ == '__main__':
    main()
