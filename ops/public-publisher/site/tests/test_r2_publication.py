"""Verify publication boundaries and recovery through the HTTP client."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/push_to_r2.py"
SPEC = importlib.util.spec_from_file_location("r2_publication", SCRIPT)
assert SPEC and SPEC.loader
publication = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = publication
with patch.object(sys, 'path', [str(SCRIPT.parent), *sys.path]):
    SPEC.loader.exec_module(publication)


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.public = self.root / "public"
        self.public.mkdir()
        (self.public / "index.html").write_text("<title>Public</title>")
        self.manifest = {"files": ["index.html", "status.json"], "required": ["index.html", "status.json"]}
        (self.public / "status.json").write_text(json.dumps({"generated_at": 1000}))

    def freeze(self, now=1000):
        return publication.freeze_public_tree(self.public, self.manifest, now=now)

    def test_only_manifest_files_are_read(self):
        (self.public / "index.html.bak").write_text("old")
        (self.public / "credentials.env").write_text("not public")
        self.assertEqual(set(self.freeze()), {"index.html", "status.json"})

    def test_rejects_symlinks_including_parent_directories(self):
        outside = self.root / "outside.html"
        outside.write_text("private")
        (self.public / "index.html").unlink()
        (self.public / "index.html").symlink_to(outside)
        with self.assertRaisesRegex(publication.PublicationError, "SYMLINK"):
            self.freeze()

    def test_fifo_does_not_block_publication(self):
        (self.public / "index.html").unlink()
        os.mkfifo(self.public / "index.html")
        with self.assertRaisesRegex(publication.PublicationError, "REGULAR_FILE"):
            self.freeze()

    def test_stale_and_future_required_json_are_rejected(self):
        for now in [999, 1181]:
            with self.subTest(now=now), self.assertRaisesRegex(publication.PublicationError, "STALE_OR_FUTURE"):
                self.freeze(now=now)

    def test_duplicate_and_nonfinite_json_are_rejected(self):
        for raw in ['{"generated_at":1000,"generated_at":1000}', '{"generated_at":NaN}']:
            (self.public / "status.json").write_text(raw)
            with self.subTest(raw=raw), self.assertRaises(publication.PublicationError):
                self.freeze()

    def test_manifest_cannot_publish_backups_or_escape_root(self):
        for name in ["../private.json", "/private.json", "index.html.bak", "assets/../index.html"]:
            self.manifest = {"files": [name], "required": []}
            with self.subTest(name=name), self.assertRaises(publication.PublicationError):
                self.freeze()

    def test_credentials_must_be_private_and_outside_public(self):
        credentials = self.root / "credentials.env"
        credentials.write_text('CLOUDFLARE_ACCOUNT_ID=' + 'a' * 32 + '\nCLOUDFLARE_API_TOKEN=test-only-token\n')
        credentials.chmod(0o644)
        with self.assertRaisesRegex(publication.PublicationError, "PERMISSIONS"):
            publication.load_credentials(credentials, self.public)
        credentials.chmod(0o600)
        value = publication.load_credentials(credentials, self.public)
        self.assertNotIn("test-only-token", repr(value))
        credentials.rename(self.public / "credentials.env")
        with self.assertRaisesRegex(publication.PublicationError, "INSIDE_PUBLIC"):
            publication.load_credentials(self.public / "credentials.env", self.public)


class HTTPPublicationTests(unittest.TestCase):
    def setUp(self):
        health = patch.object(publication, 'health_observation', return_value={'http_status':200,'status':'ok'})
        health.start(); self.addCleanup(health.stop)
        self.objects = {}
        self.requests = []
        self.drop_put_response = False
        self.discard_put = False
        self.deny = False
        self.deny_get_names = set()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                outer.requests.append(("GET", self.path))
                if outer.deny or self.path.rsplit('/',1)[-1] in outer.deny_get_names:
                    self.send_error(403)
                    return
                if self.path not in outer.objects:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.end_headers()
                self.wfile.write(outer.objects[self.path])

            def do_PUT(self):
                outer.requests.append(("PUT", self.path))
                raw = self.rfile.read(int(self.headers["Content-Length"]))
                if not outer.discard_put:
                    outer.objects[self.path] = raw
                if outer.drop_put_response:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                response = json.dumps({"success": True, "result": {"key": self.path.split("/objects/", 1)[1]}}).encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(response)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

        def local_transport(request, timeout):
            self.assertTrue(request.full_url.startswith(publication.API_BASE + "/accounts/"))
            address = f"http://127.0.0.1:{self.server.server_port}{request.selector}"
            forwarded = urllib.request.Request(address, data=request.data, headers=dict(request.headers), method=request.method)
            return urllib.request.urlopen(forwarded, timeout=timeout)

        self.client = publication.R2Client(
            publication.Credentials("a" * 32, "test-only-token"),
            "public-site", deadline=time.monotonic() + 30, opener=local_transport,
        )

    def test_upload_is_read_back_before_success_and_paths_keep_slashes(self):
        result = publication.publish(self.client, {"assets/site.js": b"public"}, {})
        self.assertEqual(result["uploaded"], 1)
        self.assertEqual([r[0] for r in self.requests], ["GET", "PUT", "GET"])
        self.assertTrue(all("/objects/public/assets/site.js" in r[1] for r in self.requests))
        self.assertFalse(any("%2F" in r[1] for r in self.requests))

    def test_unchanged_verified_content_does_not_make_network_requests(self):
        raw = b"unchanged"
        result = publication.publish(self.client, {"index.html": raw}, {"index.html": hashlib.sha256(raw).hexdigest()})
        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(self.requests, [])

    def test_known_changed_object_skips_pre_read_but_is_verified_after_put(self):
        old_hash = hashlib.sha256(b"previous verified").hexdigest()
        result = publication.publish(self.client, {"index.html": b"updated"}, {"index.html": old_hash})
        self.assertEqual(result['uploaded'], 1)
        self.assertEqual([r[0] for r in self.requests], ['PUT', 'GET'])

    def test_parallel_lost_responses_are_reconciled_once_per_object(self):
        self.drop_put_response = True
        frozen = {'first.json': b'first', 'second.json': b'second'}
        result = publication.publish(self.client, frozen, {})
        self.assertEqual(result['reconciled'], 2)
        for name in frozen:
            self.assertEqual(sum(method == 'PUT' and path.endswith('/' + name) for method, path in self.requests), 1)
            self.assertEqual(self.client.get(name), frozen[name])

    def test_parallel_deadline_does_not_issue_requests(self):
        self.client.deadline = time.monotonic() - 1
        with self.assertRaisesRegex(publication.PublicationError, 'DEADLINE_EXCEEDED'):
            publication.publish(self.client, {'one.json': b'one', 'two.json': b'two'}, {})
        self.assertEqual(self.requests, [])

    def test_existing_identical_object_is_reconciled_without_put(self):
        self.client.put("index.html", b"already committed")
        self.requests.clear()
        result = publication.publish(self.client, {"index.html": b"already committed"}, {})
        self.assertEqual(result["reconciled"], 1)
        self.assertEqual([r[0] for r in self.requests], ["GET"])

    def test_lost_upload_response_is_reconciled_without_duplicate_put(self):
        self.drop_put_response = True
        result = publication.publish(self.client, {"index.html": b"committed"}, {})
        self.assertEqual(result["reconciled"], 1)
        self.assertEqual(sum(r[0] == "PUT" for r in self.requests), 1)

    def test_unconfirmed_upload_is_a_failure_without_retry(self):
        self.drop_put_response = True
        self.discard_put = True
        with self.assertRaisesRegex(publication.PublicationError, "OUTCOME_UNCONFIRMED"):
            publication.publish(self.client, {"index.html": b"missing"}, {})
        self.assertEqual(sum(r[0] == "PUT" for r in self.requests), 1)

    def test_access_denied_does_not_become_missing_object_or_trigger_put(self):
        self.deny = True
        with self.assertRaises(publication.ProviderError) as caught:
            publication.publish(self.client, {"index.html": b"public"}, {})
        self.assertEqual(caught.exception.status, 403)
        self.assertFalse(any(r[0] == "PUT" for r in self.requests))

    def test_failed_previous_cycle_does_not_reuse_old_verified_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "public").mkdir()
            (root / "scripts").mkdir()
            (root / ".state").mkdir()
            (root / "public/index.html").write_bytes(b"old")
            (root / "scripts/r2-public-files.json").write_text(json.dumps({"files": ["index.html"], "required": ["index.html"]}))
            credentials = root / "credentials.env"
            credentials.write_text("CLOUDFLARE_ACCOUNT_ID=" + "a" * 32 + "\nCLOUDFLARE_API_TOKEN=test-only-token\n")
            credentials.chmod(0o600)
            state_path = root / ".state/public-push-r2.json"
            state_path.write_text(json.dumps({"status": "FAILED", "destination": "a" * 32 + "/public-site/public/", "failure_count": 2, "verified_sha256": {"index.html": hashlib.sha256(b"old").hexdigest()}}))
            self.client.put("index.html", b"partial-new")
            self.requests.clear()
            env = {"YUKIMURATA_R2_BUCKET": "public-site", "YUKIMURATA_R2_CREDENTIALS": str(credentials)}
            with patch.object(publication, "SITE_DIR", root), patch.object(publication, "R2Client", return_value=self.client), patch.object(publication, "build_public_tree"), patch.dict(os.environ, env), patch.object(sys, "argv", [str(SCRIPT)]), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(publication.main(), 0)
            self.assertTrue(any(r[0] == "PUT" for r in self.requests))
            status = json.loads(state_path.read_text())
            self.assertEqual(status["failure_count"], 2)
            self.assertEqual(status["status"], "SUCCEEDED")
            self.assertEqual(self.client.get("index.html"), b"old")

    def test_main_partial_cycle_keeps_live_updates_and_verified_static_receipts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); (root/'public/assets').mkdir(parents=True); (root/'scripts').mkdir()
            (root/'public/index.html').write_text('<head></head><script src="./assets/site.js"></script>')
            (root/'public/assets/site.js').write_text('public')
            live=list(publication.LIVE_FILES)
            for name in live:(root/'public'/name).write_text(json.dumps({'generated_at':time.time()}))
            manifest={'files':['index.html','assets/site.js',*live,'site-release.json'], 'required':['index.html',*live], 'live':live}
            (root/'scripts/r2-public-files.json').write_text(json.dumps(manifest))
            credentials=root/'credentials.env';credentials.write_text('CLOUDFLARE_ACCOUNT_ID='+'a'*32+'\nCLOUDFLARE_API_TOKEN=test-only-token\n');credentials.chmod(0o600)
            env={'YUKIMURATA_R2_BUCKET':'public-site','YUKIMURATA_R2_CREDENTIALS':str(credentials)}
            self.deny_get_names={'relay-observation.json'}
            with patch.object(publication,'SITE_DIR',root),patch.object(publication,'R2Client',return_value=self.client),patch.object(publication,'build_public_tree'),patch.dict(os.environ,env),patch.object(sys,'argv',[str(SCRIPT)]),contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(publication.main(),1)
                state_path=root/'.state/public-push-r2.json'
                first=json.loads(state_path.read_text());self.assertEqual(first['status'],'PARTIAL');self.assertEqual(first['failure_count'],1)
                self.assertEqual(first['errors'][0]['http_status'],403)
                self.assertGreaterEqual(first['errors'][0]['source_age_seconds'],0)
                self.assertTrue(any(p.endswith('/stream-v3-prometheus.json') for p in self.objects))
                self.assertTrue(any(p.endswith('/twitch-observation.json') for p in self.objects))
                self.assertTrue(any(p.endswith('/site-release.json') for p in self.objects))
                self.deny_get_names.clear();self.requests.clear()
                self.assertEqual(publication.main(),0)
            second=json.loads(state_path.read_text());self.assertEqual(second['failure_count'],1)
            self.assertEqual(second['last_failure_at'],first['last_failure_at'])
            self.assertFalse(any(p.endswith('/assets/site.js') for _,p in self.requests))
            self.assertEqual(sum(m=='PUT' and p.endswith('/relay-observation.json') for m,p in self.requests),1)
            self.assertNotIn('test-only-token',(root/'.state/public-push-r2.events.jsonl').read_text())


class PublicationConcurrencyTests(unittest.TestCase):
    def test_two_uploads_overlap_but_a_third_waits_and_all_are_read_back(self):
        entered = threading.Event()
        release = threading.Event()
        lock = threading.Lock()
        active = peak = 0
        writes = []
        reads = []
        objects = {}

        class Client:
            def put(self, name, raw):
                nonlocal active, peak
                with lock:
                    active += 1; peak = max(peak, active); writes.append(name)
                    if active == 2: entered.set()
                try:
                    if not release.wait(3): raise AssertionError('test did not release uploads')
                    objects[name] = raw
                finally:
                    with lock: active -= 1

            def get(self, name):
                reads.append(name)
                return objects.get(name)

        frozen = {name: name.encode() for name in ('one.json', 'two.json', 'three.json')}
        result = []; errors = []
        def run():
            try: result.append(publication.publish(Client(), frozen, {n: 'old-verified' for n in frozen}))
            except BaseException as exc: errors.append(exc)
        thread = threading.Thread(target=run)
        thread.start()
        try:
            self.assertTrue(entered.wait(2), 'two uploads never overlapped')
            with lock: self.assertEqual(len(writes), 2)
        finally:
            release.set(); thread.join(4)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual((peak, active), (2, 0))
        self.assertEqual(result[0]['uploaded'], 3)
        self.assertCountEqual(reads, frozen)

    def test_failure_waits_for_inflight_upload_before_returning(self):
        both_started = threading.Barrier(2)
        finished = threading.Event()
        calls = []
        class Client:
            def put(self, name, raw):
                calls.append(name); both_started.wait(2)
                if name == 'bad.json': raise publication.PublicationError('failed')
                time.sleep(0.03); finished.set()
            def get(self, name):
                return b'ok' if name == 'good.json' and finished.is_set() else None
        with self.assertRaisesRegex(publication.PublicationError, 'OUTCOME_UNCONFIRMED'):
            publication.publish(Client(), {'bad.json': b'bad', 'good.json': b'ok'}, {'bad.json': 'old', 'good.json': 'old'})
        self.assertTrue(finished.is_set())
        self.assertCountEqual(calls, ['bad.json', 'good.json'])


class SiteReleaseTests(unittest.TestCase):
    def fixture(self):
        return {"index.html": b'<head><link href="./assets/site.css" /></head><script type="module" src="./assets/site.js"></script>',
            "assets/site.js": b'publicScript()', "assets/site.css": b'body{}',
            "observation.json": b'{"generated_at":1000}', "stream-v3-loki.html": b'dynamic history'}

    def test_release_is_stable_across_repeated_builds_and_observation_updates(self):
        first = publication.prepare_site_release(self.fixture())
        second = publication.prepare_site_release(first)
        self.assertEqual(first, second)
        changed = dict(first, **{"observation.json": b'{"generated_at":1060}', "stream-v3-loki.html": b'new history'})
        self.assertEqual(first["site-release.json"], publication.prepare_site_release(changed)["site-release.json"])

    def test_html_script_and_style_changes_each_produce_a_new_revision(self):
        original = self.fixture()
        before = json.loads(publication.prepare_site_release(original)["site-release.json"])
        for name in ["index.html", "assets/site.js", "assets/site.css"]:
            changed = {**original, name: original[name] + b'\n<!-- changed -->'}
            after = json.loads(publication.prepare_site_release(changed)["site-release.json"])
            self.assertNotEqual(before["revision"], after["revision"], name)

    def test_page_and_assets_are_bound_to_the_announced_revision(self):
        result = publication.prepare_site_release(self.fixture())
        release = json.loads(result["site-release.json"])
        self.assertEqual(set(release["files"]), {"index.html", "assets/site.js", "assets/site.css"})
        for name, digest in release["files"].items():
            self.assertEqual(hashlib.sha256(result[name]).hexdigest(), digest)
        html = result["index.html"].decode()
        self.assertEqual(html.count('?v=' + release["revision"]), 2)
        self.assertIn('content="' + release["revision"] + '"', html)

    def test_missing_linked_asset_aborts_before_publication(self):
        files = self.fixture()
        del files["assets/site.js"]
        with self.assertRaisesRegex(publication.PublicationError, 'ASSET_MISSING'):
            publication.prepare_site_release(files)

    def test_announcement_is_published_after_all_static_readbacks(self):
        objects, calls = {}, []
        class Client:
            def get(self, name):
                calls.append(("GET", name))
                return objects.get(name)
            def put(self, name, raw):
                calls.append(("PUT", name))
                objects[name] = raw
        files = publication.prepare_site_release(self.fixture())
        publication.publish(Client(), files, {})
        release_put = calls.index(("PUT", "site-release.json"))
        self.assertTrue(all(calls.index(("GET", name), calls.index(("PUT", name))+1) < release_put
            for name in files if name != "site-release.json"))

    def test_failed_static_upload_never_announces_a_release(self):
        writes = []
        class Client:
            def get(self, name): return None
            def put(self, name, raw):
                writes.append(name)
                raise publication.PublicationError('failed')
        with self.assertRaises(publication.PublicationError):
            publication.publish(Client(), publication.prepare_site_release(self.fixture()), {})
        self.assertNotIn("site-release.json", writes)


if __name__ == "__main__":
    unittest.main()
