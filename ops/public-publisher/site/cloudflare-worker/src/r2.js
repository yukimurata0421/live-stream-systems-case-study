import {PUBLIC_SOURCES, publicationState, snapshotFreshness} from "../../public/assets/publication-freshness-20261027.js";

// Public files only. The private R2 bucket is accessed through its binding.
const PREFIX = "public/";

function objectPrefix(env) {
  const value = env.PUBLIC_R2_PREFIX ?? PREFIX;
  return typeof value === "string" && value.endsWith("/") && /^(?:[A-Za-z0-9][A-Za-z0-9_-]*\/)+$/.test(value) ? value : null;
}

async function publicationHealth(env, strictObservations = false, prefix = PREFIX) {
  const checkedAt = Date.now() / 1000;
  let page = "unavailable";
  try {
    const object = await env.SITE_BUCKET?.get(prefix + "index.html");
    if (object && object.size <= 1024 * 1024) {
      const html = await new Response(object.body).text();
      if (/type="module" src="\.\/assets\/(?:ops-20261027|ops-20261027-r2-repair|ops-20261027-notices)\.js(?:\?v=[a-f0-9]{64})?"/.test(html)) page = "available";
    }
  } catch { page = "unavailable"; }
  const sources = await Promise.all(Object.entries(PUBLIC_SOURCES).map(async ([key, contract]) => {
    let value = null, failure = null;
    try {
      if (!env.SITE_BUCKET) throw new Error("unbound");
      const object = await env.SITE_BUCKET.get(prefix + contract.file);
      if (object) {
        if (object.size > 2 * 1024 * 1024) failure = "oversized";
        else value = await new Response(object.body).json();
      }
    } catch { failure = "read_or_parse_failed"; }
    const freshness = snapshotFreshness(key, value, checkedAt);
    return {source: key, file: contract.file, required_for_live_status: contract.live === true, ...freshness, ...(failure ? {state: failure} : {})};
  }));
  const state = publicationState(Object.fromEntries(sources.map(source => [source.source, source])), page === "available");
  const healthy = strictObservations ? state.observationsComplete : state.liveDataReady;
  return new Response(JSON.stringify({schema: "public.publication_health.v1",
    scope: strictObservations ? "public_observation_snapshot_delivery" : "public_live_snapshot_delivery",
    status: healthy ? "ok" : "unhealthy", observations_status: state.observationsComplete ? "complete" : "partial",
    issues: state.issues, checked_at: checkedAt, page, sources}), {
    status: healthy ? 200 : 503,
    headers: {"Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff", "X-Public-Storage": "R2"},
  });
}

function publicPath(pathname) {
  let path;
  try {
    path = decodeURIComponent(pathname);
  } catch {
    return null;
  }
  if (path.includes("\\") || /[\u0000-\u001f\u007f]/.test(path)) return null;
  if (path.split("/").some((part) => part.startsWith("."))) return null;
  return path.endsWith("/") ? `${path}index.html` : path;
}

function contentType(path) {
  if (path.endsWith(".html")) return "text/html; charset=utf-8";
  if (path.endsWith(".css")) return "text/css; charset=utf-8";
  if (path.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (path.endsWith(".json")) return "application/json; charset=utf-8";
  return "application/octet-stream";
}

function conditionalResponse(request, response) {
  const etag = response.headers.get("ETag");
  const condition = request.headers.get("If-None-Match");
  if (etag && condition && condition.split(",").some((value) => {
    const candidate = value.trim().replace(/^W\//, "");
    return candidate === "*" || candidate === etag;
  })) {
    return new Response(null, { status: 304, headers: response.headers });
  }
  return request.method === "HEAD"
    ? new Response(null, { status: response.status, headers: response.headers })
    : response;
}

export default {
  async fetch(request, env, ctx) {
    if (!["GET", "HEAD"].includes(request.method)) {
      return new Response("Method Not Allowed", {
        status: 405,
        headers: { Allow: "GET, HEAD", "Cache-Control": "no-store" },
      });
    }
    const prefix = objectPrefix(env);
    if (!prefix) return new Response("Public prefix unavailable", {status: 503, headers: {"Cache-Control": "no-store"}});
    const url = new URL(request.url);
    if (["/health/publication", "/health/observations"].includes(url.pathname)) {
      const response = await publicationHealth(env, url.pathname === "/health/observations", prefix);
      return request.method === "HEAD" ? new Response(null, {status: response.status, headers: response.headers}) : response;
    }
    const path = publicPath(url.pathname);
    if (!path) return new Response("Bad Request", { status: 400 });
    if (!env.SITE_BUCKET) {
      return new Response("Public storage unavailable", {
        status: 503, headers: { "Cache-Control": "no-store" },
      });
    }

    // Read R2 directly: Cache API hits can inherit a zone browser TTL of four
    // hours. Client cache headers below retain the 60s/300s public contract.
    let object;
    try {
      object = await env.SITE_BUCKET.get(`${prefix}${path.slice(1)}`);
    } catch {
      return new Response("Public storage unavailable", {
        status: 503, headers: { "Cache-Control": "no-store" },
      });
    }
    if (!object) {
      return new Response("Not Found", {
        status: 404, headers: { "Cache-Control": "no-store" },
      });
    }

    const headers = new Headers();
    const ttl = path.endsWith(".json") ? 60 : 300;
    headers.set("Content-Type", contentType(path));
    headers.set("Cache-Control", path === "/site-release.json" ? "no-store" : `public, max-age=${ttl}`);
    headers.set("Content-Length", String(object.size));
    headers.set("ETag", object.httpEtag);
    headers.set("Last-Modified", object.uploaded.toUTCString());
    headers.set("Referrer-Policy", "no-referrer");
    headers.set("X-Content-Type-Options", "nosniff");
    headers.set("X-Public-Storage", "R2");
    const response = new Response(object.body, { headers });
    return conditionalResponse(request, response);
  },
};
