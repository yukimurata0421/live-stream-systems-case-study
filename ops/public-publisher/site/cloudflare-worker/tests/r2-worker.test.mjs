import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const shared = await readFile(new URL("../../public/assets/publication-freshness-20261027.js", import.meta.url), "utf8");
const sharedUrl = `data:text/javascript;base64,${Buffer.from(shared).toString("base64")}`;
const {PUBLIC_SOURCES, publicationState, snapshotFreshness, snapshotView} = await import(sharedUrl);
const source = (await readFile(process.env.R2_WORKER_TEST_BUNDLE || new URL("../src/r2.js", import.meta.url), "utf8"))
  .replace('"../../public/assets/publication-freshness-20261027.js"', JSON.stringify(sharedUrl));
const { default: worker } = await import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);
globalThis.fetch = () => { throw new Error("External origin fetch is forbidden"); };

test("the configured prefix is confined and matches the publisher convention", async () => {
  const f = fixture();
  f.env.PUBLIC_R2_PREFIX = "status/site/";
  f.objects.set("status/site/index.html", "custom prefix");
  assert.equal(await (await f.call("/")).text(), "custom prefix");
  assert.deepEqual(f.reads, ["status/site/index.html"]);
  for (const prefix of ["", "/", "../", "public/../", "public//", "public/\n", "public/\r", 7]) {
    const invalid = fixture();
    invalid.env.PUBLIC_R2_PREFIX = prefix;
    assert.equal((await invalid.call("/")).status, 503);
    assert.deepEqual(invalid.reads, []);
  }
});

test("the packaged public page satisfies the publication health entrypoint check", async () => {
  const f = healthFixture();
  const html = await readFile(new URL("../../public/index.html", import.meta.url), "utf8");
  f.objects.set("public/index.html", html);
  assert.equal((await f.call("/health/publication")).status, 200);
});

function fixture() {
  const objects = new Map([
    ["public/index.html", "<title>Public</title>"],
    ["public/status.json", '{"generated_at":1000}'],
    ["public/assets/site.js", "publicScript()"],
  ]);
  const reads = [];
  const storage = new Map();
  const pending = [];
  globalThis.caches = { default: {
    match: async (key) => storage.get(key.url)?.clone(),
    put: async (key, value) => { storage.set(key.url, value.clone()); },
  } };
  const env = { SITE_BUCKET: { get: async (key) => {
    reads.push(key);
    if (!objects.has(key)) return null;
    const body = objects.get(key);
    return { body, size: Buffer.byteLength(body), httpEtag: '"example-etag"', uploaded: new Date("2026-10-27T00:00:00Z") };
  } } };
  const ctx = { waitUntil: (promise) => pending.push(promise) };
  return { env, reads, storage, objects, call: async (path, options) => {
    const response = await worker.fetch(new Request(`https://example.invalid${path}`, options), env, ctx);
    await Promise.all(pending.splice(0));
    return response;
  } };
}

test("root is served from the private bucket and never from GCS", async () => {
  const f = fixture();
  const response = await f.call("/");
  assert.equal(response.status, 200);
  assert.equal(await response.text(), "<title>Public</title>");
  assert.deepEqual(f.reads, ["public/index.html"]);
  assert.equal(response.headers.get("x-public-storage"), "R2");
  assert.equal(response.headers.get("cache-control"), "public, max-age=300");
});

test("JSON has a 60-second lifetime and the correct MIME type", async () => {
  const response = await fixture().call("/status.json");
  assert.equal(response.headers.get("cache-control"), "public, max-age=60");
  assert.equal(response.headers.get("content-type"), "application/json; charset=utf-8");
  assert.equal((await response.json()).generated_at, 1000);
});

test("HEAD preserves object headers and omits the body", async () => {
  const response = await fixture().call("/assets/site.js", { method: "HEAD" });
  assert.equal(response.status, 200);
  assert.equal(await response.text(), "");
  assert.equal(response.headers.get("etag"), '"example-etag"');
  assert.equal(response.headers.get("content-length"), String(Buffer.byteLength("publicScript()")));
});

test("unsafe methods never reach R2", async () => {
  const f = fixture();
  const response = await f.call("/", { method: "POST", body: "ignored" });
  assert.equal(response.status, 405);
  assert.equal(response.headers.get("allow"), "GET, HEAD");
  assert.deepEqual(f.reads, []);
});

test("unbound and failed storage return 503 without origin fallback", async () => {
  const f = fixture();
  delete f.env.SITE_BUCKET;
  assert.equal((await f.call("/")).status, 503);
  f.env.SITE_BUCKET = { get: async () => { throw new Error("private provider detail"); } };
  const response = await f.call("/");
  assert.equal(response.status, 503);
  assert.equal(await response.text(), "Public storage unavailable");
});

test("404 responses are not cached, allowing newly published files", async () => {
  const f = fixture();
  assert.equal((await f.call("/new.json")).status, 404);
  f.objects.set("public/new.json", "{}");
  assert.equal((await f.call("/new.json")).status, 200);
});

test("encoded traversal, hidden files, and malformed paths are refused", async () => {
  const f = fixture();
  for (const path of ["/.env", "/assets%2f..%2fprivate.json", "/%00", "/%zz", "/assets%5cprivate.json"]) {
    assert.equal((await f.call(path)).status, 400, path);
  }
  assert.deepEqual(f.reads, []);
});

test("each request reads R2 directly and arbitrary queries cannot change its key", async () => {
  const f = fixture();
  await f.call("/?one=1");
  f.objects.set("public/index.html", "<title>Updated</title>");
  const response = await f.call("/?two=2");
  assert.equal(await response.text(), "<title>Updated</title>");
  assert.deepEqual(f.reads, ["public/index.html", "public/index.html"]);
  assert.equal(f.storage.size, 0);
});

test("zone Cache API behavior cannot extend the JSON lifetime", async () => {
  const f = fixture();
  globalThis.caches.default.match = () => { throw new Error("Zone would return a four-hour cached response"); };
  globalThis.caches.default.put = () => { throw new Error("Zone would rewrite the cached TTL"); };
  for (const method of ["GET", "HEAD"]) {
    const response = await f.call("/status.json", { method });
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("cache-control"), "public, max-age=60");
  }
});

test("conditional GET and HEAD return 304 using the current R2 entity tag", async () => {
  const f = fixture();
  await f.call("/");
  for (const method of ["GET", "HEAD"]) {
    const response = await f.call("/", { method, headers: { "If-None-Match": 'W/"example-etag"' } });
    assert.equal(response.status, 304);
    assert.equal(await response.text(), "");
  }
});

function healthFixture() {
  const f = fixture();
  f.objects.set("public/index.html", '<script type="module" src="./assets/ops-20261027.js"></script>');
  for (const contract of Object.values(PUBLIC_SOURCES)) {
    const value = {schema: contract.schema, generated_at: Date.now() / 1000};
    for (const list of contract.lists || []) value[list] = [];
    f.objects.set("public/" + contract.file, JSON.stringify(value));
  }
  return f;
}

test("publication health checks all eight JSON feeds and is never cached", async () => {
  const response = await healthFixture().call("/health/publication");
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("cache-control"), "no-store");
  const result = await response.json();
  assert.equal(result.status, "ok");
  assert.equal(result.scope, "public_live_snapshot_delivery");
  assert.equal(result.observations_status, "complete");
  assert.equal(result.sources.length, 8);
  assert.ok(result.sources.every(value => value.state === "fresh"));
});

test("every feed defect remains visible while only live-status sources fail primary health", async () => {
  for (const contract of Object.values(PUBLIC_SOURCES)) {
    for (const problem of ["stale", "future", "missing", "invalid_json", "wrong_schema", "wrong_shape"]) {
      const f = healthFixture(), key = "public/" + contract.file;
      const value = JSON.parse(f.objects.get(key));
      if (problem === "stale") value.generated_at -= 600;
      if (problem === "future") value.generated_at += 600;
      if (problem === "wrong_schema") value.schema = "unexpected.v1";
      if (problem === "wrong_shape" && contract.lists?.length) value[contract.lists[0]] = {};
      else if (problem === "wrong_shape") value.generated_at = false;
      f.objects.set(key, problem === "invalid_json" ? "{" : JSON.stringify(value));
      if (problem === "missing") f.objects.delete(key);
      const response = await f.call("/health/publication");
      assert.equal(response.status, contract.live ? 503 : 200, `${contract.file}: ${problem}`);
      const result = await response.json();
      assert.equal(result.status, contract.live ? "unhealthy" : "ok");
      assert.equal(result.observations_status, "partial");
      assert.equal(result.issues.length, 1);
      const strict = await f.call("/health/observations");
      assert.equal(strict.status, 503, `${contract.file}: ${problem}`);
      assert.equal((await strict.json()).scope, "public_observation_snapshot_delivery");
    }
  }
});

test("health recovers after a stale JSON is replaced without a cached failure", async () => {
  const f = healthFixture(), key = "public/reliability-indicators.json";
  const fresh = f.objects.get(key), value = JSON.parse(fresh);
  value.generated_at -= 600;
  f.objects.set(key, JSON.stringify(value));
  assert.equal((await f.call("/health/publication")).status, 200);
  assert.equal((await f.call("/health/observations")).status, 503);
  f.objects.set(key, fresh);
  assert.equal((await f.call("/health/publication")).status, 200);
  assert.equal((await f.call("/health/observations")).status, 200);
  assert.equal((await f.call("/health/publication", {method: "HEAD"})).status, 200);
});

test("publication health fails for an unavailable bucket or page", async () => {
  const f = healthFixture();
  f.objects.delete("public/index.html");
  assert.equal((await f.call("/health/publication")).status, 503);
  delete f.env.SITE_BUCKET;
  const response = await f.call("/health/publication");
  assert.equal(response.status, 503);
  assert.equal(response.headers.get("cache-control"), "no-store");
});

test("freshness boundaries accept numeric and ISO timestamps and reject invalid values", () => {
  const now = 1793059000, schema = PUBLIC_SOURCES.twitch.schema;
  for (const [offset, expected] of [[-181, "stale"], [-180, "fresh"], [0, "fresh"], [30, "fresh"], [31, "future"]]) {
    for (const generated_at of [now + offset, new Date((now + offset) * 1000).toISOString()]) {
      assert.equal(snapshotFreshness("twitch", {schema, generated_at}, now).state, expected);
    }
  }
  for (const generated_at of [null, true, Infinity, NaN, "1793059000", "bad date", 0]) {
    assert.equal(snapshotFreshness("twitch", {schema, generated_at}, now).state, "invalid_timestamp");
  }
});

test("a frozen previously fresh view expires without resetting or mutating its history", () => {
  const raw = {rel: {schema: PUBLIC_SOURCES.rel.schema, generated_at: 1793059000, items: [{value: 99.12}], tiers: []}};
  const original = JSON.stringify(raw);
  assert.equal(snapshotView(raw, 1793059000).rel.items[0].value, 99.12);
  assert.equal(snapshotView(raw, 1793059181).rel, null);
  assert.equal(snapshotView(raw, 1793059181).freshness.rel.state, "stale");
  assert.equal(JSON.stringify(raw), original);
});

test("page and worker use the same critical-source classification without hiding auxiliary gaps", () => {
  const fresh = Object.fromEntries(Object.keys(PUBLIC_SOURCES).map(key => [key, {state: "fresh"}]));
  assert.equal(publicationState(fresh).liveDataReady, true);
  for (const key of Object.keys(PUBLIC_SOURCES)) {
    const value = {...fresh, [key]: {state: "stale"}};
    const result = publicationState(value);
    assert.equal(result.liveDataReady, PUBLIC_SOURCES[key].live !== true, key);
    assert.equal(result.observationsComplete, false);
    assert.deepEqual(result.issues, [key]);
  }
  assert.equal(publicationState(fresh, false).liveDataReady, false);
});

test("new page entrypoint and HEAD work for both health contracts", async () => {
  for (const script of ["ops-20261027-r2-repair.js", "ops-20261027-notices.js"]) {
  const f = healthFixture();
  f.objects.set("public/index.html", `<script type="module" src="./assets/${script}"></script>`);
  f.objects.delete("public/clock-observation.json");
  for (const [path, status] of [["/health/publication", 200], ["/health/observations", 503]]) {
    const response = await f.call(path, {method: "HEAD"});
    assert.equal(response.status, status);
    assert.equal(await response.text(), "");
    assert.equal(response.headers.get("cache-control"), "no-store");
  }
  }
});

test("release announcements bypass caches", async () => {
  const f = fixture();
  f.objects.set("public/site-release.json", '{"revision":"test"}');
  const response = await f.call("/site-release.json");
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("cache-control"), "no-store");
});

test("publication health accepts versioned assets and rejects arbitrary entrypoints", async () => {
  const f = healthFixture();
  for (const name of ['ops-20261027.js','ops-20261027-notices.js']) {
    f.objects.set("public/index.html", `<script type="module" src="./assets/${name}?v=${'a'.repeat(64)}"></script>`);
    assert.equal((await f.call("/health/publication")).status, 200);
  }
  f.objects.set("public/index.html", '<script type="module" src="./assets/unknown.js"></script>');
  assert.equal((await f.call("/health/publication")).status, 503);
});
