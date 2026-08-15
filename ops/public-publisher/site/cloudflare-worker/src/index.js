const ORIGIN_BASE = "https://storage.googleapis.com/addevlab-adsb-open-status/yukimurata0421";
const ORIGIN_CACHE_VERSION = "2026-06-13T01-04Z";

function normalizePath(pathname) {
  if (pathname.includes("..")) return null;
  if (pathname === "/" || pathname.endsWith("/")) return `${pathname}index.html`;
  return pathname;
}

function contentType(pathname) {
  if (pathname.endsWith(".html")) return "text/html; charset=utf-8";
  if (pathname.endsWith(".css")) return "text/css; charset=utf-8";
  if (pathname.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (pathname.endsWith(".json")) return "application/json; charset=utf-8";
  return "application/octet-stream";
}

export default {
  async fetch(request) {
    if (!["GET", "HEAD"].includes(request.method)) {
      return new Response("Method Not Allowed", { status: 405, headers: { Allow: "GET, HEAD" } });
    }

    const url = new URL(request.url);
    const path = normalizePath(url.pathname);
    if (!path) return new Response("Bad Request", { status: 400 });

    const originUrl = new URL(`${ORIGIN_BASE}${path}`);
    originUrl.searchParams.set("__v", ORIGIN_CACHE_VERSION);
    const ttl = path.endsWith(".json") ? 60 : 300;
    const response = await fetch(originUrl.toString(), {
      cf: {
        cacheEverything: true,
        cacheTtl: ttl,
      },
    });

    const headers = new Headers(response.headers);
    headers.set("Cache-Control", `public, max-age=${ttl}`);
    headers.set("Content-Type", headers.get("Content-Type") || contentType(path));
    headers.set("Referrer-Policy", "no-referrer");
    headers.set("X-Content-Type-Options", "nosniff");

    return new Response(request.method === "HEAD" ? null : response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  },
};
