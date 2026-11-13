(() => {
  // assets/site-auto-reload-20261028.js
  function startAutoReload({
    document: doc = document,
    window: win = window,
    fetch: request = fetch,
    crypto: crypt = crypto
  } = {}) {
    const loaded = doc.querySelector('meta[name="site-revision"]')?.content;
    if (!/^[a-f0-9]{64}$/.test(loaded || "")) return;
    const base = new URL("./", win.location.href);
    let checking = false, leaving = false;
    const hash = async (bytes) => [...new Uint8Array(await crypt.subtle.digest("SHA-256", bytes))].map((value) => value.toString(16).padStart(2, "0")).join("");
    async function read(path, signal, limit) {
      const url = new URL(path, base);
      url.searchParams.set("_release_check", Date.now());
      const response = await request(url.href, { cache: "no-store", signal, redirect: "error" });
      if (!response.ok || Number(response.headers.get("Content-Length")) > limit) throw new Error("release unavailable");
      const bytes = await response.arrayBuffer();
      if (!bytes.byteLength || bytes.byteLength > limit) throw new Error("release too large");
      return bytes;
    }
    async function check() {
      if (checking || leaving || doc.visibilityState !== "visible") return;
      checking = true;
      const controller = new AbortController();
      const timeout = win.setTimeout(() => controller.abort(), 2e4);
      try {
        const bytes = await read("site-release.json", controller.signal, 32768);
        const release = JSON.parse(new TextDecoder().decode(bytes));
        if (release.schema !== "public.site_release.v1" || !/^[a-f0-9]{64}$/.test(release.revision || "")) return;
        if (release.revision === loaded || new URL(win.location.href).searchParams.get("_site_revision") === release.revision) return;
        if (!release.files || typeof release.files !== "object" || Array.isArray(release.files)) return;
        const files = Object.entries(release.files);
        if (!files.length || files.length > 80 || !release.files["index.html"] || !files.some(([path]) => path.endsWith(".js"))) return;
        if (files.some(([path, digest]) => !/^(?:index\.html|assets\/[a-zA-Z0-9_-][a-zA-Z0-9_.-]*\.(?:js|css))$/.test(path) || typeof digest !== "string" || !/^[a-f0-9]{64}$/.test(digest))) return;
        let cursor = 0, total = 0;
        const verify = async () => {
          while (cursor < files.length) {
            const [path, digest] = files[cursor++];
            const content = await read(path, controller.signal, 8 * 1024 * 1024);
            total += content.byteLength;
            if (total > 16 * 1024 * 1024 || await hash(content) !== digest) throw new Error("release incomplete");
          }
        };
        await Promise.all([verify(), verify()]);
        if (doc.visibilityState !== "visible") return;
        const destination = new URL(win.location.href);
        destination.searchParams.set("_site_revision", release.revision);
        leaving = true;
        win.location.replace(destination.href);
      } catch {
      } finally {
        controller.abort();
        win.clearTimeout(timeout);
        checking = false;
      }
    }
    win.setInterval(check, 6e4);
    doc.addEventListener("visibilitychange", check);
    win.addEventListener("online", check);
    win.addEventListener("pageshow", check);
    void check();
  }

  // assets/publication-freshness-20261027.js
  var PUBLIC_SOURCES = Object.freeze({
    prom: { file: "stream-v3-prometheus.json", schema: "stream-v3-prometheus-public.v1", label: "Monitoring", live: true, lists: ["items"] },
    loki: { file: "stream-v3-loki.json", schema: "stream-v3-loki-public.v1", label: "Event history", lists: ["sections"] },
    rel: { file: "reliability-indicators.json", schema: "stream-v3-reliability-public.v6", label: "Reliability", lists: ["items", "tiers"] },
    relay: { file: "relay-observation.json", schema: "stream.relay_public.v1", label: "Relay observation", live: true },
    twitch: { file: "twitch-observation.json", schema: "stream-v3-twitch-public.v1", label: "Twitch observation", live: true },
    tsli: { file: "twitch-sli.json", schema: "stream.twitch_sli_public.v1", label: "Twitch history", lists: ["windows", "timeline"] },
    clock: { file: "clock-observation.json", schema: "stream.clock_observation_public.v1", label: "Clock observations", lists: ["sources"] },
    egress: { file: "oci-egress.json", schema: "stream.oci_egress_public.v1", label: "Data transfer", lists: ["days"] }
  });
  var MAX_SNAPSHOT_AGE_SECONDS = 180;
  var FUTURE_TOLERANCE_SECONDS = 30;
  function snapshotFreshness(key, value, nowSeconds) {
    const contract = PUBLIC_SOURCES[key];
    if (!contract) throw new Error("Unknown public source");
    let state = "missing", generatedAt = null, ageSeconds = null;
    if (value !== null && value !== void 0) {
      if (typeof value !== "object" || Array.isArray(value) || value.schema !== contract.schema || (contract.lists || []).some((name) => !Array.isArray(value[name]))) {
        state = "invalid_schema";
      } else {
        const stamp = value.generated_at;
        const parsed = typeof stamp === "number" ? stamp : typeof stamp === "string" && /^\d{4}-\d{2}-\d{2}T/.test(stamp) ? Date.parse(stamp) / 1e3 : NaN;
        if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isFinite(nowSeconds)) {
          state = "invalid_timestamp";
        } else {
          generatedAt = parsed;
          ageSeconds = nowSeconds - parsed;
          state = ageSeconds < -FUTURE_TOLERANCE_SECONDS ? "future" : ageSeconds > MAX_SNAPSHOT_AGE_SECONDS ? "stale" : "fresh";
        }
      }
    }
    return { state, generatedAt, ageSeconds };
  }
  function snapshotView(raw, nowSeconds) {
    const view = { freshness: {} };
    for (const key of Object.keys(PUBLIC_SOURCES)) {
      const value = raw?.[key];
      const result = snapshotFreshness(key, value, nowSeconds);
      view.freshness[key] = result;
      view[key] = result.state === "fresh" ? value : null;
    }
    return view;
  }

  // assets/ops-20261027.js
  (() => {
    const SOURCES = Object.fromEntries(Object.entries(PUBLIC_SOURCES).map(([key, value]) => [key, value.file]));
    const FRESH_WARN = 180, FRESH_BAD = 300, RANK = { good: 0, unknown: 1, warning: 2, serious: 3, critical: 4 };
    const $ = (id) => document.getElementById(id);
    const SVGNS = "http://www.w3.org/2000/svg";
    const el = (tag, attrs = {}, text) => {
      const n = tag.startsWith("svg:") ? document.createElementNS(SVGNS, tag.slice(4)) : document.createElement(tag);
      for (const [k, v] of Object.entries(attrs)) if (v !== void 0 && v !== null) n.setAttribute(k, v);
      if (text !== void 0) n.textContent = text;
      return n;
    };
    const clear = (n) => {
      while (n.firstChild) n.removeChild(n.firstChild);
      return n;
    };
    const finite = (v) => typeof v === "number" && Number.isFinite(v);
    const now = () => Date.now() / 1e3;
    const toSec = (v) => finite(v) ? v : typeof v === "string" ? Date.parse(v) / 1e3 : NaN;
    const jst = (sec, opts = { hour: "2-digit", minute: "2-digit" }) => new Date(sec * 1e3).toLocaleString("en-GB", { timeZone: "Asia/Tokyo", hour12: false, ...opts });
    const ago = (sec) => {
      if (!finite(sec)) return "unknown";
      const d = Math.max(0, now() - sec);
      if (d < 90) return `${Math.round(d)} s ago`;
      if (d < 5400) return `${Math.round(d / 60)} min ago`;
      const h = Math.floor(d / 3600), m = Math.round(d % 3600 / 60);
      return h < 48 ? `${h} h ${m} min ago` : `${Math.round(h / 24)} days ago`;
    };
    const until = (sec) => {
      const d = sec - now();
      if (!finite(d) || d < 0) return "";
      const h = Math.floor(d / 3600), m = Math.round(d % 3600 / 60);
      return h ? `in ${h} h ${m} min` : `in ${m} min`;
    };
    const mbps = (v) => finite(v) ? `${v.toFixed(2)} Mbps` : "not measured";
    const pct = (v, d = 2) => finite(v) ? `${v.toFixed(d)}%` : "\u2014";
    const sliFresh = (d, t = now()) => {
      return d.tsli?.schema === "stream.twitch_sli_public.v1" && freshTime(d.tsli.generated_at, t);
    };
    const worst = (states) => states.reduce((a, b) => (RANK[b] ?? 1) > (RANK[a] ?? 1) ? b : a, "good");
    const tip = $("tooltip");
    function showTip(evt, text) {
      tip.textContent = text;
      tip.hidden = false;
      const x = Math.min(window.innerWidth - tip.offsetWidth - 8, evt.clientX + 12);
      const y = Math.max(8, evt.clientY - tip.offsetHeight - 12);
      tip.style.left = `${x}px`;
      tip.style.top = `${y}px`;
    }
    const hideTip = () => {
      tip.hidden = true;
    };
    async function load() {
      const entries = await Promise.all(Object.entries(SOURCES).map(async ([key, file]) => {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 8e3);
        try {
          const r = await fetch(`./${file}?t=${Date.now()}`, { cache: "no-store", signal: controller.signal });
          return [key, r.ok ? await r.json() : null];
        } catch {
          return [key, null];
        } finally {
          clearTimeout(timeout);
        }
      }));
      return Object.fromEntries(entries);
    }
    function sourceIssue(d, key) {
      const f = d.freshness?.[key], label = PUBLIC_SOURCES[key].label;
      if (f?.state === "stale") return `${label} updates stopped \xB7 last ${jst(f.generatedAt)} JST (${ago(f.generatedAt)})`;
      if (f?.state === "future") return `${label} timestamp is in the future`;
      if (f?.state === "invalid_schema" || f?.state === "invalid_timestamp") return `${label} data is invalid`;
      return `${label} data is unavailable`;
    }
    function freshTime(stamp, t, limit = FRESH_WARN) {
      const age = t - toSec(stamp);
      return finite(age) && age >= -30 && age <= limit;
    }
    function model(d) {
      const t = now();
      const promAge = t - toSec(d.prom?.generated_at);
      const items = new Map((d.prom?.items || []).map((i) => [i.id, i]));
      const relayFresh = d.relay?.schema === "stream.relay_public.v1" && d.relay.status === "OBSERVED" && d.relay.source_fresh === true && freshTime(d.relay.generated_at, t);
      const fb = d.relay?.fallback;
      const fbKnown = fb && typeof fb.active === "boolean" && freshTime(fb.observed_at, t);
      const fallback = fbKnown ? fb.active : null;
      const input = relayFresh ? d.relay.input : null;
      const dest = relayFresh ? d.relay.destinations : {};
      const ytFlags = ["youtube_issue", "ingest_issue", "same_url_issue"].map((id) => items.get(id)).filter(Boolean);
      const ytKnown = ytFlags.length === 3 && ytFlags.every((i) => finite(i.value));
      const ytBad = ytFlags.filter((i) => finite(i.value) && i.value > 0);
      const ytRecent = items.get("current_fail_1h");
      const twState = d.twitch?.observation?.provider_state || "UNKNOWN";
      const twFresh = d.twitch?.observation?.source_fresh === true;
      const daily = relayFresh ? d.relay.daily : null;
      const renewing = daily ? daily.planned_break === true || ["WAIT_STOP", "WAIT_OFFLINE", "WAIT_START", "VERIFYING"].includes(daily.state) : inRenewalWindow(t);
      const relayHealthy = relayFresh && input?.status === "FLOWING" && dest?.youtube?.status === "FLOWING";
      const fallbackReady = fbKnown ? fb.ready === true : null;
      const candidate = fb?.controller;
      const controller = candidate?.schema === "stream_v3.fallback_controller_public.v1" && freshTime(candidate.observed_at, t, 90) && t - toSec(candidate.observed_at) >= -1 && ["HEALTHY", "UNHEALTHY", "UNKNOWN"].includes(candidate.health) && finite(candidate.healthy_for_sec) && candidate.healthy_for_sec >= 0 && candidate.stop_after_sec === 300 ? candidate : null;
      return { t, d, items, promAge, relayFresh, relayHealthy, fallback, fallbackReady, controller, input, dest, ytFlags, ytKnown, ytBad, ytRecent, twState, twFresh, renewing };
    }
    function controllerReason(code) {
      const names = {
        progress_confirmed: "relay progress confirmed",
        baseline_required: "waiting for a second observation",
        facts_timeout: "observation request timed out",
        facts_http_unavailable: "observation service unavailable",
        facts_transport_error: "observation connection unavailable",
        observation_unavailable: "observations unavailable",
        copy_facts_missing: "YouTube output observations unavailable",
        copy_not_flowing: "YouTube output not confirmed",
        input_not_live: "live input not confirmed",
        input_not_ready: "relay input not ready",
        input_not_advancing: "relay input is not advancing",
        output_not_advancing: "YouTube output is not advancing",
        output_not_forwarding: "YouTube forwarding is not confirmed",
        video_not_advancing: "video packets are not advancing",
        audio_not_advancing: "audio packets are not advancing",
        connection_changed: "connection changed; checking again",
        sample_gap: "observation gap; checking again",
        source_future: "relay observation clock is ahead",
        copy_future: "output observation clock is ahead",
        progress_future: "output progress clock is ahead",
        source_stale: "relay observation is stale",
        copy_stale: "output observation is stale",
        progress_stale: "output progress is stale"
      };
      return names[code] || "observation validation failed";
    }
    function fallbackMessage(m) {
      const c = m.controller;
      if (!m.fallbackReady) return "Direct backup requested; the sender is not ready. " + (c ? `Controller: ${controllerReason(c.reason)}.` : "Controller status is unavailable or stale.");
      if (!c) return "Direct backup is running. Controller status is unavailable or stale; recovery timing is not confirmed.";
      if (c.health === "HEALTHY") return `Direct backup is running. The controller has confirmed ${Math.floor(c.healthy_for_sec)} of ${c.stop_after_sec} seconds of continuous relay progress required to stop it.`;
      return `Direct backup is running. Controller: ${controllerReason(c.reason)}. ` + (c.last_reset_at ? "The continuous recovery check has reset." : "Continuous recovery is not confirmed.");
    }
    function inRenewalWindow(sec) {
      const h = Number(jst(sec, { hour: "2-digit" })), m = Number(jst(sec, { minute: "2-digit" }));
      return h === 0 && m < 15;
    }
    function assess(m) {
      const reasons = [];
      const add = (state2, text) => reasons.push({ state: state2, text });
      for (const key of Object.keys(PUBLIC_SOURCES).filter((key2) => PUBLIC_SOURCES[key2].live)) {
        if (m.d.freshness?.[key]?.state !== "fresh") add("unknown", sourceIssue(m.d, key) + ".");
      }
      if (!m.ytKnown) add("unknown", "YouTube checks are unavailable.");
      for (const i of m.ytBad) add("critical", `YouTube: ${i.label} (${formatItem(i)}).`);
      if (finite(m.ytRecent?.value) && m.ytRecent.value > 0)
        add("warning", `YouTube had ${m.ytRecent.value} interruption${m.ytRecent.value > 1 ? "s" : ""} in the last hour; it is live now.`);
      if (m.fallback === true)
        add(m.controller?.health === "UNHEALTHY" ? "serious" : "warning", fallbackMessage(m));
      else if (m.relayFresh && m.input?.status !== "FLOWING") add("serious", "The relay is not receiving the upload from Dell.");
      else if (!m.relayFresh) add("unknown", "Relay observation is unavailable.");
      if (m.relayFresh && m.input?.status === "FLOWING") {
        for (const [name, label] of [["youtube", "YouTube"], ["twitch", "Twitch"]])
          if (m.dest?.[name]?.status !== "FLOWING") {
            if (name === "twitch" && m.renewing) {
              if (m.twState !== "OFFLINE") add("warning", "Twitch daily session renewal is in progress; the relay output to Twitch is paused.");
            } else add("serious", `Relay output to ${label} is not progressing.`);
          }
      }
      if (m.twState === "OFFLINE") add(
        m.renewing ? "warning" : "serious",
        m.renewing ? "Twitch daily session renewal is in progress." : "Twitch is offline."
      );
      else if (m.twState !== "LIVE" || !m.twFresh) add("unknown", "Twitch status is not confirmed.");
      const tr = transferModel(m.d, m.t);
      if (tr?.fresh && tr.usedRatio >= 1) add("serious", "OCI outbound data transfer is over the 10 TB monthly allowance.");
      else if (tr?.fresh && tr.projHiRatio >= 1) add("warning", "OCI outbound data transfer is on pace to exceed the 10 TB monthly allowance.");
      const critical = (m.d.rel?.active_incidents?.items || []).filter((i) => String(i.severity).toLowerCase() === "critical");
      if (critical.length) add("warning", `${critical.length} critical reliability alert${critical.length > 1 ? "s" : ""} (see Incidents).`);
      const decision = /* @__PURE__ */ new Set(["youtube_issue", "ingest_issue", "same_url_issue", "current_fail_1h", "ffmpeg_clusters_1h"]);
      for (const i of m.items.values()) if (!decision.has(i.id) && ["bad", "warn"].includes(i.state)) add("warning", `${i.label}: ${formatItem(i)}.`);
      let state = worst(reasons.map((r) => r.state));
      if (reasons.some((r) => r.state === "unknown") && RANK[state] < RANK.serious) state = "unknown";
      return { state, reasons };
    }
    function renderStatus(m) {
      const { state, reasons } = assess(m);
      const title = { unknown: "Live status unconfirmed", good: "Live status normal", warning: "Running \xB7 needs attention", serious: "Degraded", critical: "Disrupted" }[state];
      const icon = { unknown: "?", good: "\u2713", warning: "!", serious: "!", critical: "\xD7" }[state];
      $("status").className = `status-banner state-${state}`;
      $("statusIcon").textContent = icon;
      $("statusTitle").textContent = title;
      const list = clear($("statusReasons"));
      if (!reasons.length) list.append(el("li", {}, "YouTube and Twitch are live through the relay with a single upload."));
      for (const r of reasons.sort((a, b) => RANK[b.state] - RANK[a.state]).slice(0, 4)) list.append(el("li", {}, r.text));
      $("statusUpdated").textContent = finite(m.promAge) ? `Monitoring updated ${ago(toSec(m.d.prom.generated_at))}` : "Monitoring unavailable";
    }
    function setBadge(id, state, text) {
      const b = $(id);
      b.dataset.state = state;
      b.textContent = text;
    }
    function facts(id, rows) {
      const dl = clear($(id));
      for (const [k, v] of rows) if (v !== void 0 && v !== null && v !== "") dl.append(el("dt", {}, k), el("dd", {}, v));
    }
    function routeName(m) {
      if (m.fallback === true && m.relayHealthy) return "OCI relay + direct backup";
      if (m.fallback === true) return "Direct backup requested";
      if (m.relayFresh && m.input?.status === "FLOWING") return "OCI relay";
      return "not confirmed";
    }
    function renderNow(m) {
      const ytOk = m.ytKnown && !m.ytBad.length;
      setBadge("ytBadge", m.ytBad.length ? "critical" : ytOk ? "good" : "unknown", m.ytBad.length ? "ISSUE" : ytOk ? "LIVE" : "UNKNOWN");
      $("ytValue").textContent = m.ytBad.length ? "Problem detected" : ytOk ? "Live" : "Unknown";
      const up = m.items.get("upload_p95_1h");
      facts("ytFacts", [
        ["Upload paths", routeName(m)],
        ["Relay output", m.relayFresh ? mbps(m.dest?.youtube?.mbps) : "not observed"],
        ["Upload p95 (1 h)", up ? formatItem(up) : null],
        ["Stream URL", m.items.get("same_url_issue")?.value === 0 ? "unchanged" : m.items.get("same_url_issue") ? "check" : null],
        ["Interruptions (1 h)", m.ytRecent ? formatItem(m.ytRecent) : null]
      ]);
      const tw = m.d.twitch;
      const twGood = m.twState === "LIVE" && m.twFresh;
      setBadge(
        "twBadge",
        twGood ? "good" : m.twState === "OFFLINE" ? m.renewing ? "warning" : "serious" : "unknown",
        twGood ? "LIVE" : m.twState === "OFFLINE" ? m.renewing ? "RENEWING" : "OFFLINE" : "UNKNOWN"
      );
      $("twValue").textContent = twGood ? "Live" : m.twState === "OFFLINE" ? "Offline" : "Unknown";
      const next = toSec(m.d.relay?.daily?.next_renewal_at);
      facts("twFacts", [
        ["Session started", tw?.session?.started_at ? ago(toSec(tw.session.started_at)) : null],
        ["Next renewal", finite(next) ? `00:00 JST, ${until(next)}` : "daily at 00:00 JST"],
        ["Relay output", m.relayFresh ? mbps(m.dest?.twitch?.mbps) : "not observed"],
        ["Viewers", finite(tw?.session?.viewer_count) ? String(tw.session.viewer_count) : null]
      ]);
      const vm = m.relayFresh ? m.d.relay.vm : null;
      if (m.fallback === true) {
        setBadge(
          "routeBadge",
          m.controller?.health === "UNHEALTHY" ? "serious" : "warning",
          m.controller?.health === "HEALTHY" ? "RECOVERY CHECK" : m.controller?.health === "UNKNOWN" || !m.controller ? "OBSERVATION UNKNOWN" : "FALLBACK"
        );
        const fbRate = m.d.relay?.fallback?.mbps, mainRate = m.relayFresh ? m.input?.mbps : null;
        $("routeValue").textContent = finite(fbRate) && fbRate > 0 && finite(mainRate) && mainRate > 0 ? `2 uploads \xB7 ${(fbRate + mainRate).toFixed(2)} Mbps` : finite(fbRate) && fbRate > 0 ? `Backup upload \xB7 ${fbRate.toFixed(2)} Mbps` : "Direct backup requested";
        $("routeNote").textContent = fallbackMessage(m);
      } else if (m.relayFresh && m.input?.status === "FLOWING") {
        setBadge("routeBadge", "good", "SINGLE UPLOAD");
        $("routeValue").textContent = mbps(m.input.mbps);
        $("routeNote").textContent = "If the relay fails, a direct feed to YouTube starts automatically.";
      } else {
        setBadge("routeBadge", "unknown", "UNKNOWN");
        $("routeValue").textContent = "Not confirmed";
        $("routeNote").textContent = "Relay observation is missing or stale.";
      }
      facts("routeFacts", [
        ["Relay", m.relayFresh ? `${m.d.relay.relay?.status === "RUNNING" ? "running" : "stopped"} \xB7 ${m.d.relay.relay?.restart_count ?? "?"} restarts` : "not observed"],
        ["Direct fallback", m.fallback === true ? `active \xB7 ${mbps(m.d.relay?.fallback?.mbps)}` : m.fallback === false ? "standby" : "unknown"],
        ["Controller check", m.controller ? controllerReason(m.controller.reason) : "unavailable or stale"],
        [m.fallback === true ? "Continuous recovery" : "Continuous relay health", m.controller ? m.fallback === true ? `${Math.floor(m.controller.healthy_for_sec)} / ${m.controller.stop_after_sec} s (last check)` : `${Math.floor(m.controller.healthy_for_sec)} s (last check)` : "not confirmed"],
        ["Controller checked", m.controller ? `${jst(toSec(m.controller.observed_at), { hour: "2-digit", minute: "2-digit", second: "2-digit" })} JST` : null],
        ["Last recovery reset", m.controller?.last_reset_at ? `${jst(toSec(m.controller.last_reset_at), { hour: "2-digit", minute: "2-digit", second: "2-digit" })} JST \xB7 ${controllerReason(m.controller.last_reset_reason)}` : null],
        ["OCI CPU / memory", vm ? `${pct(vm.cpu_percent, 0)} / ${pct(vm.memory_percent, 0)}` : null],
        ["OCI disk free", vm ? pct(vm.disk_free_percent, 0) : null]
      ]);
      renderPath(m);
    }
    function renderPath(m) {
      const svg = $("pathSvg");
      [...svg.querySelectorAll("g,path,text")].forEach((n) => n.remove());
      const linkState = (s) => !m.relayFresh ? "unknown" : s === "FLOWING" ? "good" : "critical";
      const add = (d, cls) => svg.append(el("svg:path", { d, class: `link ${cls}` }));
      add("M170 113 H300", linkState(m.input?.status));
      add("M450 113 C515 113 515 48 580 48", linkState(m.dest?.youtube?.status));
      add("M450 113 C515 113 515 178 580 178", linkState(m.dest?.twitch?.status));
      add("M95 85 C95 18 420 6 580 30", m.fallback === true ? "active" : "standby");
      const node = (x, y, title, sub) => {
        const g = el("svg:g", { class: "node" });
        g.append(el("svg:rect", { x, y, width: 150, height: 56, rx: 10 }), el("svg:text", { x: x + 14, y: y + 24 }, title), el("svg:text", { x: x + 14, y: y + 43, class: "sub" }, sub));
        svg.append(g);
      };
      node(20, 85, "Dell", "encoder + sender");
      node(300, 85, "OCI relay", m.relayFresh ? m.d.relay.relay?.status === "RUNNING" ? "running" : "stopped" : "not observed");
      node(580, 20, "YouTube", "primary ingest");
      node(580, 150, "Twitch", "daily session");
      const label = (x, y, text, anchor = "middle") => svg.append(el("svg:text", { x, y, class: "link-label", "text-anchor": anchor }, text));
      const rate = (s) => m.relayFresh ? mbps(s?.mbps) : "not observed";
      label(235, 105, rate(m.input));
      label(500, 70, rate(m.dest?.youtube), "end");
      label(500, 170, rate(m.dest?.twitch), "end");
      label(330, 22, m.fallback === true ? `direct fallback \xB7 ${mbps(m.d.relay?.fallback?.mbps)}` : "direct fallback \xB7 standby");
    }
    const LANE_W = 1e3;
    function addLane(root, label, sub, value, build, short = false) {
      const row = el("div", { class: "lane" });
      const lab = el("div", { class: "lane-label" }, label);
      lab.append(el("small", {}, sub));
      const svg = el("svg:svg", { viewBox: `0 0 ${LANE_W} ${short ? 22 : 44}`, preserveAspectRatio: "none", class: short ? "short" : "", role: "img", "aria-label": `${label}, last 24 hours` });
      build(svg, short ? 22 : 44);
      row.append(lab, svg, el("div", { class: "lane-value" }, value));
      root.append(row);
    }
    function addAxis(root, t0, t1, x) {
      const axis = el("div", { class: "axis-row" });
      const ticks = el("div", { class: "axis-ticks" });
      for (let s = Math.ceil(t0 / 10800) * 10800; s <= t1; s += 10800) {
        const span = el("span", {}, jst(s, { hour: "2-digit", minute: "2-digit" }));
        span.style.left = `${x(s) / LANE_W * 100}%`;
        ticks.append(span);
      }
      axis.append(el("div"), ticks, el("div"));
      root.append(axis);
    }
    function renderTimeline(m) {
      const root = clear($("timeline"));
      const t1 = m.t, t0 = t1 - 86400, W = LANE_W;
      const x = (s) => (s - t0) / (t1 - t0) * W;
      const lane = (...args) => addLane(root, ...args);
      const current = sliFresh(m.d, m.t), updated = toSec(m.d.tsli?.generated_at);
      const timelineLabel = current ? `by hour \xB7 updated ${jst(updated)} JST` : sourceIssue(m.d, "tsli");
      const tl = (current ? m.d.tsli?.timeline || [] : []).map((b) => ({ ...b, s: toSec(b.start_at) })).filter((b) => finite(b.s) && b.s + 3600 > t0);
      let good = 0, bad = 0;
      for (const b of tl) {
        good += b.good || 0;
        bad += b.bad || 0;
      }
      lane("Twitch live", timelineLabel, good + bad ? pct(100 * good / (good + bad), 1) : "\u2014", (svg, h) => {
        for (const b of tl) {
          const known = (b.good || 0) + (b.bad || 0);
          const st = !known ? "unknown" : b.bad === 0 ? "good" : b.bad > 30 ? "critical" : "warning";
          const r = el("svg:rect", { x: Math.max(0, x(b.s)) + 1, y: 3, width: Math.max(2, x(b.s + 3600) - Math.max(0, x(b.s)) - 2), height: h - 6, class: `cell ${st}` });
          const text = `${jst(b.s)}\u2013${jst(b.s + 3600)} JST \xB7 live ${b.good || 0} min \xB7 offline ${b.bad || 0} min${b.unknown || b.missing ? ` \xB7 no data ${(b.unknown || 0) + (b.missing || 0)} min` : ""}`;
          r.addEventListener("mousemove", (e) => showTip(e, text));
          r.addEventListener("mouseleave", hideTip);
          svg.append(r);
        }
      }, true);
      const trend = (id) => ((m.d.prom?.trends || []).find((tr) => tr.id === id)?.points || []).filter((p) => finite(p.ts) && p.ts >= t0);
      const up = trend("upload_p95_24h");
      const latest = up.length ? up[up.length - 1].value : null;
      lane("Upload bitrate", "p95 \xB7 dashed = 5 Mbps ceiling", finite(latest) ? `${latest.toFixed(2)} Mbps` : "\u2014", (svg, h) => {
        const vals = up.map((p) => p.value).filter(finite);
        const vmax = Math.max(5.5, ...vals), vmin = Math.max(0, Math.min(4, ...vals) - 0.5);
        const y = (v) => h - 3 - (v - vmin) / (vmax - vmin) * (h - 8);
        svg.append(el("svg:line", { x1: 0, x2: W, y1: y(vmin), y2: y(vmin), class: "gridline" }));
        svg.append(el("svg:line", { x1: 0, x2: W, y1: y(5), y2: y(5), class: "ref" }));
        let d = "", pen = false;
        for (const p of up) {
          if (!finite(p.value)) {
            pen = false;
            continue;
          }
          d += `${pen ? "L" : "M"}${x(p.ts).toFixed(1)} ${y(p.value).toFixed(1)}`;
          pen = true;
        }
        svg.append(el("svg:path", { d, class: "line", "vector-effect": "non-scaling-stroke" }));
        crosshair(svg, h, up, x, (p) => `${jst(p.ts)} JST \xB7 upload p95 ${finite(p.value) ? p.value.toFixed(2) + " Mbps" : "no data"}`);
      });
      const upt = trend("ffmpeg_uptime_24h");
      const restarts = [];
      for (let i = 1; i < upt.length; i++) if (finite(upt[i].value) && finite(upt[i - 1].value) && upt[i].value < upt[i - 1].value) restarts.push(upt[i]);
      lane("Encoder restarts", "detected from uptime resets", String(restarts.length), (svg, h) => {
        svg.append(el("svg:line", { x1: 0, x2: W, y1: h / 2, y2: h / 2, class: "gridline" }));
        for (const p of restarts) {
          const start = p.ts - p.value;
          const g = el("svg:g");
          g.append(el("svg:line", { x1: x(start), x2: x(start), y1: 3, y2: h - 3, class: "tick", "vector-effect": "non-scaling-stroke" }));
          const hit = el("svg:rect", { x: x(start) - 8, y: 0, width: 16, height: h, class: "hit" });
          hit.addEventListener("mousemove", (e) => showTip(e, `Encoder (re)started around ${jst(start)} JST`));
          hit.addEventListener("mouseleave", hideTip);
          g.append(hit);
          svg.append(g);
        }
      }, true);
      addAxis(root, t0, t1, x);
    }
    function crosshair(svg, h, pts, x, text) {
      if (!pts.length) return;
      const cursor = el("svg:line", { y1: 0, y2: h, class: "cursor", visibility: "hidden", "vector-effect": "non-scaling-stroke" });
      const hit = el("svg:rect", { x: 0, y: 0, width: 1e3, height: h, class: "hit" });
      hit.addEventListener("mousemove", (e) => {
        const box = svg.getBoundingClientRect();
        const px = (e.clientX - box.left) / box.width * 1e3;
        let best = pts[0];
        for (const p of pts) if (Math.abs(x(p.ts) - px) < Math.abs(x(best.ts) - px)) best = p;
        cursor.setAttribute("x1", x(best.ts));
        cursor.setAttribute("x2", x(best.ts));
        cursor.setAttribute("visibility", "visible");
        showTip(e, text(best));
      });
      hit.addEventListener("mouseleave", () => {
        cursor.setAttribute("visibility", "hidden");
        hideTip();
      });
      svg.append(cursor, hit);
    }
    const CLOCK_TEXT = {
      ADVANCING_CONFIRMED: "Clock moving",
      ADVANCING_PROBABLE: "Probably moving",
      WARMUP: "Starting up",
      STALLED_SUSPECT: "Possibly frozen",
      STALLED_CONFIRMED: "Frozen",
      BLACK_FRAME: "Black picture",
      OCR_UNCERTAIN: "Clock not readable",
      UNKNOWN: "Not observed"
    };
    const THUMB_TEXT = {
      FRESH: ["good", "FRESH", "Up to date"],
      AGING: ["good", "AGING", "Aging, as expected"],
      STALE_SUSPECT: ["warning", "STALE?", "Possibly stale"],
      STALE_CORRELATED: ["serious", "STALE", "Stale while the relay sends"],
      SOURCE_OFFLINE: ["unknown", "OFFLINE", "Twitch is offline"],
      WAITING_NEW_STREAM_THUMBNAIL: ["unknown", "WAITING", "Waiting for the new session's image"],
      WAITING_POLL: ["unknown", "WAITING", "Waiting for the next check"],
      WAITING_RETRY: ["unknown", "RETRYING", "Retrying after a refused request"],
      UNKNOWN: ["unknown", "UNKNOWN", "Not observed"]
    };
    const CLASS_BADGE = { target_good: ["good", "MOVING"], target_bad: ["critical", "PROBLEM"], observation_unknown: ["unknown", "NOT READ"], standby: ["unknown", "STANDBY"] };
    const secs = (v) => !finite(v) ? null : v < 90 ? `${v.toFixed(v < 10 ? 1 : 0)} s` : `${Math.round(v / 60)} min`;
    const count = (v) => finite(v) ? v.toLocaleString("en-US") : "\u2014";
    const cellState = (c) => !c ? "unknown" : c.bad >= 0.5 ? "critical" : c.bad > 0 ? "warning" : c.good >= Math.max(c.unknown, c.standby) ? "good" : c.standby >= c.unknown ? "standby" : "unknown";
    const share = (c) => !c ? "no data" : [["moving", c.good], ["problem", c.bad], ["not read", c.unknown], ["standby", c.standby]].filter(([, v]) => v > 0).map(([k, v]) => `${k} ${Math.round(v * 100)}%`).join(" \xB7 ");
    function clockTile(root, title, sub, badge, value, rows, note, more = []) {
      const tile = el("article", { class: "tile" });
      const head = el("header");
      const h = el("h3", {}, title);
      h.append(el("small", { class: "tile-sub" }, sub));
      const b = el("span", { class: "badge", "data-state": badge[0] }, badge[1]);
      head.append(el("span", { class: "dot dot-route", "aria-hidden": "true" }), h, b);
      const dl = el("dl", { class: "kv" });
      for (const [k, v] of rows) if (v !== void 0 && v !== null && v !== "") dl.append(el("dt", {}, k), el("dd", {}, v));
      tile.append(head, el("p", { class: "tile-value" }, value), dl);
      const extra = more.filter(([, v]) => v !== void 0 && v !== null && v !== "");
      if (extra.length) {
        const det = el("details", { class: "tile-more" }), mdl = el("dl", { class: "kv" });
        for (const [k, v] of extra) mdl.append(el("dt", {}, k), el("dd", {}, v));
        det.append(el("summary", {}, "OCR read counts"), mdl);
        tile.append(det);
      }
      tile.append(el("span", { class: "tile-note" }, note));
      root.append(tile);
    }
    function renderClocks(d) {
      const v = d.clock, t = now();
      const gen = toSec(v?.generated_at);
      const fresh = v?.schema === "stream.clock_observation_public.v1" && finite(gen) && t - gen >= -30 && t - gen <= FRESH_WARN;
      $("clockStamp").textContent = fresh ? `Measured ${jst(gen)} JST \xB7 ${ago(gen)}` : sourceIssue(d, "clock");
      const src = (id) => fresh ? (v.sources || []).find((s) => s.id === id) : null;
      const tiles = clear($("clockTiles"));
      const stale = ["unknown", fresh ? "UNKNOWN" : "NO DATA"];
      const relay = src("oci_relay");
      const reads = (r) => r && finite(r.valid_clock) ? `${count(r.valid_clock)} valid \xB7 ${count(r.ocr_uncertain)} unclear` : null;
      clockTile(
        tiles,
        "Relay output",
        "Dell \u2192 OCI relay \u2192 Dell \xB7 every 60\xA0s",
        relay ? CLASS_BADGE[relay.classification] || stale : stale,
        relay ? CLOCK_TEXT[relay.state] || "Not observed" : "Not observed",
        [
          ["Clock delay now", relay ? secs(relay.clock_delay_seconds) : null],
          ["Delay p50 / p95 (24 h)", relay && finite(relay.delay_p50_24h) ? `${secs(relay.delay_p50_24h)} / ${secs(relay.delay_p95_24h)}` : null],
          ["Latest frame", relay && finite(relay.sample_age_seconds) ? `${secs(relay.sample_age_seconds)} ago` : null]
        ],
        "Reads the picture the relay sends to YouTube and Twitch. It is not what viewers receive.",
        [["Reads (24 h)", reads(relay?.reads_24h)], ["Cadence drift (24 h)", relay?.reads_24h ? count(relay.reads_24h.cadence_drift) : null]]
      );
      const fb = src("youtube_fallback");
      const fbStandby = fb?.classification === "standby";
      clockTile(
        tiles,
        "Direct YouTube fallback",
        "Dell \u2192 YouTube backup \xB7 while sending",
        fb ? CLASS_BADGE[fb.classification] || stale : stale,
        !fb ? "Not observed" : fbStandby ? "Standby" : CLOCK_TEXT[fb.state] || "Not observed",
        [
          ["Fallback", fb ? fb.expected_active === true ? "sending" : fb.expected_active === false ? "not sending" : "unknown" : null],
          ["Last clock read", fb && finite(fb.since_valid_seconds) ? ago(gen - fb.since_valid_seconds) : null]
        ],
        "Checked only while the fallback is sending; standby is the normal state.",
        [["Reads (24 h)", reads(fb?.reads_24h)]]
      );
      const th = src("twitch_thumbnail");
      const [thState, thBadge, thText] = th ? THUMB_TEXT[th.state] || THUMB_TEXT.UNKNOWN : stale.concat("Not observed");
      clockTile(
        tiles,
        "Twitch thumbnail",
        "Twitch's preview image \xB7 every 60\xA0s",
        [thState, thBadge],
        thText,
        [
          ["Picture age", th ? secs(th.frame_age_seconds) : null],
          ["Image", th ? th.clock_verified ? th.image_unchanged ? "same as last check" : "new image" : "not observed" : null],
          ["Last clock read", th && finite(th.since_valid_seconds) ? `about ${ago(gen - th.since_valid_seconds)}` : null],
          ["New images (24 h)", th ? count(th.samples_24h) : null]
        ],
        "Twitch refreshes this image about every 6 minutes, so ages up to 14 min are expected. It shows Twitch received the picture, not player playback.",
        [["Reads (24 h)", th && finite(th.reads_24h) ? `${count(th.reads_24h)} \xB7 ${count(th.retries_24h)} re-read` : null]]
      );
      const root = clear($("clockTimeline"));
      const step = fresh ? v.bucket_seconds : 600, first = fresh ? v.bucket_start : t - 86400;
      const t0 = first - step, t1 = first + step * 143;
      const x = (s) => (s - t0) / (t1 - t0) * LANE_W;
      const strip = (label, sub, s) => {
        const cells = s?.timeline || [];
        const known = cells.filter((c) => c && c.good + c.bad > 0);
        const moving = known.length ? known.reduce((a, c) => a + c.good / (c.good + c.bad), 0) / known.length : null;
        const seen = cells.filter((c) => c && c.unknown < 0.5).length / 144;
        addLane(root, label, sub, finite(moving) ? `${pct(100 * moving, 1)} \xB7 ${pct(100 * seen, 0)} read` : "\u2014", (svg, h) => {
          cells.forEach((c, i) => {
            const end = first + i * step;
            const r = el("svg:rect", { x: x(end - step) + 0.5, y: 3, width: Math.max(1, x(end) - x(end - step) - 1), height: h - 6, class: `cell ${cellState(c)}` });
            const text = `${jst(end - step)}\u2013${jst(end)} JST \xB7 ${share(c)}`;
            r.addEventListener("mousemove", (e) => showTip(e, text));
            r.addEventListener("mouseleave", hideTip);
            svg.append(r);
          });
        }, true);
      };
      const line = (label, sub, values, refs = [], observation = null) => {
        const pts = (values || []).map((value, i) => ({ ts: first + i * step, value }));
        const vals = pts.map((p) => p.value).filter(finite);
        const latest = pts.length ? pts[pts.length - 1].value : null;
        const observed = observation === null || observation.clock_verified === true;
        const value = !observed ? "Not observed" : finite(latest) ? secs(latest) : "\u2014";
        const lastRead = observation && finite(observation.since_valid_seconds) ? gen - observation.since_valid_seconds : null;
        if (!observed && finite(lastRead)) sub += ` \xB7 last clock read about ${ago(lastRead)}`;
        if (observation && observed && finite(latest)) sub += ` \xB7 through ${jst(pts[pts.length - 1].ts)} JST`;
        addLane(root, label, sub, value, (svg, h) => {
          const vmax = Math.max(...refs.map((r) => r * 1.1), ...vals, 1);
          const y = (val) => h - 3 - val / vmax * (h - 8);
          svg.append(el("svg:line", { x1: 0, x2: LANE_W, y1: y(0), y2: y(0), class: "gridline" }));
          for (const r of refs) svg.append(el("svg:line", { x1: 0, x2: LANE_W, y1: y(r), y2: y(r), class: "ref" }));
          let path = "", pen = false;
          for (const p of pts) {
            if (!finite(p.value)) {
              pen = false;
              continue;
            }
            path += `${pen ? "L" : "M"}${x(p.ts).toFixed(1)} ${y(p.value).toFixed(1)}`;
            pen = true;
          }
          svg.append(el("svg:path", { d: path, class: "line", "vector-effect": "non-scaling-stroke" }));
          if (observed && finite(latest)) svg.append(el("svg:circle", { cx: x(pts[pts.length - 1].ts), cy: y(latest), r: 3,
            fill: "currentColor", class: "current-observation" }));
          crosshair(svg, h, pts, x, (p) => `${jst(p.ts - step)}\u2013${jst(p.ts)} JST \xB7 ${label.toLowerCase()} ${finite(p.value) ? secs(p.value) : "no data"}`);
        });
      };
      strip("Relay output", "clock moving, by 10 min", relay);
      strip("Direct fallback", "grey = standby", fb);
      strip("Twitch thumbnail", "clock read and recent", th);
      line("Relay clock delay", "on-screen clock \u2192 decoded frame", relay?.value_series);
      line("Thumbnail age", "10-min max \xB7 dashed 6 and 14 min", th?.value_series, [360, 840], th);
      addAxis(root, t0, t1, x);
    }
    const factValue = (f) => finite(f.value) ? `${Number.isInteger(f.value) ? f.value.toLocaleString("en-US") : f.value.toLocaleString("en-US", { maximumFractionDigits: 3 })}${f.unit && f.unit !== "%" ? " " + f.unit : f.unit || ""}` : String(f.value ?? "\u2014");
    const VERDICT_STATE = { MET: "good", HOLDING: "good", BELOW: "warning", BREACHED: "serious", EXCEEDED: "serious", UNKNOWN: "unknown", COLLECTING: "unknown" };
    const TIERS = [["slo", "Viewer-facing SLOs"], ["quality", "Quality references"], ["guardrail", "Guardrails & invariants"]];
    function relRows(d) {
      if (d.rel?.schema !== "stream-v3-reliability-public.v6") return [];
      const labels = new Map((d.rel.tiers || []).map((t) => [t.id, t.label]));
      const rows = [];
      for (const it of d.rel.items || []) {
        const pctRow = it.unit === "%";
        rows.push({
          tier: it.tier,
          tierLabel: labels.get(it.tier),
          name: it.label,
          window: it.window,
          value: pctRow ? pct(it.value, 2) : finite(it.value) ? `${factValue({ value: it.value })} ${it.unit || ""}` : "\u2014",
          pctValue: pctRow ? it.value : null,
          recent: finite(it.recent_24h) ? it.recent_24h : null,
          target: finite(it.target) ? it.target : null,
          state: VERDICT_STATE[it.assessment?.status] || "unknown",
          label: it.assessment?.label || "not judged",
          facts: it.facts || [],
          note: it.note
        });
      }
      const order = new Map(TIERS.map(([id], i) => [id, i]));
      return rows.sort((x, y) => (order.get(x.tier) ?? 9) - (order.get(y.tier) ?? 9));
    }
    function renderReliability(d) {
      const body = clear($("relRows"));
      const gen = toSec(d.rel?.generated_at);
      $("relStamp").textContent = finite(gen) ? `Measured ${jst(gen)} JST \xB7 ${ago(gen)}` : sourceIssue(d, "rel");
      const rows = relRows(d);
      if (!rows.length) {
        const tr = el("tr");
        tr.append(el("td", { colspan: 6, class: "empty state unknown" }, "UNKNOWN \xB7 Reliability data is unavailable; current values are not confirmed."));
        body.append(tr);
        return;
      }
      let tier = null;
      for (const r of rows) {
        if (r.tier !== tier) {
          tier = r.tier;
          const head = el("tr", { class: "group" });
          head.append(el("th", { colspan: 6, scope: "rowgroup" }, r.tierLabel || (TIERS.find(([id]) => id === tier) || [, "Other"])[1]));
          body.append(head);
        }
        const tr = el("tr", { class: "row", tabindex: "0", "aria-expanded": "false" });
        const valueCell = el("td", { class: "num" }, r.value);
        if (finite(r.recent)) valueCell.append(el("small", { class: "recent" }, `24 h ${pct(r.recent, r.recent % 1 ? 1 : 0)}`));
        if (r.sub) valueCell.append(el("small", { class: "recent" }, r.sub));
        tr.append(
          el("td", {}, r.name),
          el("td", { class: "window" }, r.window || ""),
          valueCell,
          el("td", { class: "num" }, finite(r.target) ? pct(r.target, r.target % 1 ? 1 : 0) : "\u2014")
        );
        const barCell = el("td");
        if (finite(r.pctValue) && finite(r.target)) {
          const lowest = Math.min(r.pctValue, r.target, finite(r.recent) ? r.recent : 100);
          const lo = lowest >= 90.5 ? 90 : Math.max(0, Math.floor((lowest - 0.5) / 10) * 10);
          const pos = (v) => `${Math.max(0, Math.min(100, (v - lo) / (100 - lo) * 100))}%`;
          const scale = `scale ${lo}\u2013100%`;
          const bar = el("div", { class: "gap-bar", title: `${r.value} (7 d)${finite(r.recent) ? `, ${pct(r.recent, 1)} (24 h)` : ""} vs ${pct(r.target, 1)} target \xB7 ${scale}` });
          const fill = el("i");
          fill.style.width = pos(r.pctValue);
          const mark = el("b");
          mark.style.left = pos(r.target);
          bar.append(fill, mark);
          if (finite(r.recent)) {
            const dot = el("u", { title: `Last 24 hours: ${pct(r.recent, 2)}` });
            dot.style.left = pos(r.recent);
            bar.append(dot);
          }
          barCell.append(bar, el("small", { class: "scale" }, scale));
        }
        tr.append(barCell, (() => {
          const [head, ...more] = String(r.label).split(" \xB7 ");
          const td2 = el("td", { class: "status" });
          td2.append(el("span", { class: `state ${r.state}` }, head));
          if (more.length) td2.append(el("small", { class: "recent" }, more.join(" \xB7 ")));
          if (finite(r.recent) && finite(r.target) && finite(r.pctValue) && r.recent < r.target && r.recent < r.pctValue)
            td2.append(el("small", { class: "recent" }, `24 h ${(r.target - r.recent).toFixed(2)} pt below`));
          return td2;
        })());
        const det = el("tr", { class: "details", hidden: "" });
        const td = el("td", { colspan: 6 });
        const dl = el("dl");
        for (const f of r.facts) {
          const div = el("div");
          div.append(el("dt", {}, f.label), el("dd", {}, factValue(f)));
          dl.append(div);
        }
        td.append(dl);
        if (r.note) td.append(el("p", {}, r.note));
        det.append(td);
        const toggle = () => {
          det.hidden = !det.hidden;
          tr.setAttribute("aria-expanded", String(!det.hidden));
        };
        tr.addEventListener("click", toggle);
        tr.addEventListener("keydown", (e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            toggle();
          }
        });
        body.append(tr, det);
      }
    }
    const INCIDENTS = {
      "reliability:youtube_availability_multi_window_burn": ["YouTube availability below target", "Delivery gaps used error budget faster than allowed in both short and long windows."],
      "reliability:youtube_input_quality_multi_window_burn": ["YouTube input quality below target", "YouTube reported ingest health problems more often than allowed."],
      "reliability:audio_correctness_multi_window_burn": ["Audio checks below target", "Audio checks failed more often than allowed in both short and long windows."],
      "reliability:upload_ceiling_coverage_or_freshness": ["Upload measurement gap", "Upload bitrate measurements were missing or stale for part of the window."],
      "external:blackbox_failed": ["External uptime check failed", "Independent cloud checkers could not confirm this page or the YouTube video from outside the home network."]
    };
    function renderIncidents(d) {
      const list = clear($("incidentList"));
      const proj = d.rel?.active_incidents;
      if (!proj || proj.status !== "ok") {
        list.append(el("li", { class: "empty" }, "Alert state unavailable."));
      } else if (!(proj.items || []).length) {
        list.append(el("li", { class: "empty" }, "No active alerts."));
      }
      for (const inc of proj?.items || []) {
        const [name, why] = INCIDENTS[inc.id] || [String(inc.id || "Alert").replace(/^reliability:/, "").replaceAll("_", " "), ""];
        const sev = String(inc.severity || "unknown").toLowerCase();
        const state = sev === "critical" ? "critical" : sev === "warning" ? "warning" : "unknown";
        const li = el("li");
        li.append(el("span", { class: `state ${state}` }, sev), el("b", { title: inc.id || "" }, name));
        const seen = toSec(inc.observed_at);
        const burn = String(inc.id || "").endsWith("_multi_window_burn") ? "covers the past 1\u20136 h; may stay listed up to 1 h after the cause clears" : "";
        li.append(el("small", {}, [why, finite(seen) ? `evaluated ${jst(seen)} JST` : "", burn, inc.recovery_type === "human_review_no_automatic_restart" ? "operator review, no automatic restart" : ""].filter(Boolean).join(" \xB7 ")));
        list.append(li);
      }
      const act = d.loki?.recovery_activity;
      const daily = Object.entries(act?.daily_counts || {}).sort(([a], [b]) => a.localeCompare(b));
      const chart = clear($("recChart"));
      $("recTotal").textContent = act ? `\xB7 ${act.total_executed ?? "?"} in ${act.period?.days ?? 7} days` : "";
      $("recBreakdown").textContent = act?.action_counts ? Object.entries(act.action_counts).filter(([, v]) => v > 0).map(([k, v]) => `${k.replaceAll("_", " ")} ${v}`).join(" \xB7 ") + ". Counts include planned work such as route changes." : "Restart activity unavailable.";
      if (!daily.length) {
        chart.append(el("p", { class: "empty" }, "No data."));
        return;
      }
      const W = 420, H = 150, pad = 22, max = Math.max(1, ...daily.map(([, v]) => v));
      const svg = el("svg:svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": "Process restart events per day" });
      const bw = W / daily.length * 0.62;
      daily.forEach(([day, v], i) => {
        const cx = W / daily.length * (i + 0.5);
        const h = (H - pad - 16) * v / max;
        const bar = el("svg:rect", { x: cx - bw / 2, y: H - pad - h, width: bw, height: Math.max(h, v ? 2 : 0), rx: 4, class: "bar" });
        bar.addEventListener("mousemove", (e) => showTip(e, `${day} \xB7 ${v} restart event${v === 1 ? "" : "s"}`));
        bar.addEventListener("mouseleave", hideTip);
        svg.append(
          bar,
          el("svg:text", { x: cx, y: H - pad - h - 4, "text-anchor": "middle", class: "bar-value" }, String(v)),
          el("svg:text", { x: cx, y: H - 6, "text-anchor": "middle", class: "bar-label" }, day.slice(5).replace("-", "/"))
        );
      });
      svg.append(el("svg:line", { x1: 0, x2: W, y1: H - pad, y2: H - pad, class: "gridline" }));
      chart.append(svg);
    }
    function renderReconnects(d) {
      const chart = $("reconChart");
      if (!chart) return;
      clear(chart);
      const rc = d.prom?.output_reconnects;
      const rows = rc?.daily || [];
      const shown = rows.filter((r) => finite(r.reconnects));
      $("reconTotal").textContent = shown.length ? `\xB7 ${rc.total ?? shown.reduce((s, r) => s + r.reconnects, 0)} in ${rc.period?.days ?? 7} days` : "";
      $("reconNote").textContent = "The FFmpeg process stayed the same and opened a new output connection, counted from the encoder host's event journal since FIFO recovery was enabled on 2026-10-24 at 12:05 JST. A reconnect is a transport fact, not proof that video or audio arrived. * marks a partial day; \u2014 marks a day before counting began.";
      if (!shown.length) {
        chart.append(el("p", { class: "empty" }, rc ? "Not observed yet." : "Reconnect data unavailable."));
        return;
      }
      const W = 420, H = 150, pad = 22, max = Math.max(1, ...shown.map((r) => r.reconnects));
      const svg = el("svg:svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": "Output reconnects per day" });
      const bw = W / rows.length * 0.62;
      rows.forEach((r, i) => {
        const cx = W / rows.length * (i + 0.5), day = String(r.date || "");
        const label = el("svg:text", { x: cx, y: H - 6, "text-anchor": "middle", class: "bar-label" }, day.slice(5).replace("-", "/") + (r.partial ? "*" : ""));
        if (!finite(r.reconnects)) {
          svg.append(el("svg:text", { x: cx, y: H - pad - 4, "text-anchor": "middle", class: "bar-label" }, "\u2014"), label);
          return;
        }
        const v = r.reconnects, h = (H - pad - 16) * v / max;
        const bar = el("svg:rect", { x: cx - bw / 2, y: H - pad - h, width: bw, height: Math.max(h, v ? 2 : 0), rx: 4, class: "bar" });
        bar.addEventListener("mousemove", (e) => showTip(e, `${day} \xB7 ${v} reconnect${v === 1 ? "" : "s"}${r.partial ? " \xB7 partial day" : ""}`));
        bar.addEventListener("mouseleave", hideTip);
        svg.append(bar, el("svg:text", { x: cx, y: H - pad - h - 4, "text-anchor": "middle", class: "bar-value" }, String(v)), label);
      });
      svg.append(el("svg:line", { x1: 0, x2: W, y1: H - pad, y2: H - pad, class: "gridline" }));
      chart.append(svg);
    }
    const TRANSFER_STALE = 3600;
    const bytes = (v) => !finite(v) ? "\u2014" : v >= 1e12 ? `${(v / 1e12).toFixed(v >= 1e13 ? 1 : 2)} TB` : v >= 1e9 ? `${(v / 1e9).toFixed(v >= 1e11 ? 0 : 1)} GB` : `${Math.round(v / 1e6)} MB`;
    const roundBytes = (v) => v >= 1e12 ? `${parseFloat((v / 1e12).toFixed(2))} TB` : `${parseFloat((v / 1e9).toFixed(1))} GB`;
    const untilDays = (sec) => {
      const d = sec - now();
      if (!finite(d) || d < 0) return "";
      const h = Math.floor(d % 86400 / 3600);
      return (d < 86400 ? `${h} h ${Math.floor(d % 3600 / 60)} min` : `${Math.floor(d / 86400)} d ${h} h`).replaceAll(" ", "\xA0");
    };
    function transferModel(d, t) {
      const v = d.egress;
      if (v?.schema !== "stream.oci_egress_public.v1" || !finite(v.quota_bytes) || v.quota_bytes <= 0) return null;
      const observed = toSec(v.observed_at), start = toSec(v.month_start), end = toSec(v.month_end);
      const projLo = Math.min(v.month_projected_egress_bytes, v.month_projected_egress_bytes_24h_rate);
      const projHi = Math.max(v.month_projected_egress_bytes, v.month_projected_egress_bytes_24h_rate);
      return {
        v,
        observed,
        start,
        end,
        projLo,
        projHi,
        fresh: freshTime(observed, t, TRANSFER_STALE),
        usedRatio: v.month_egress_bytes / v.quota_bytes,
        projHiRatio: projHi / v.quota_bytes
      };
    }
    function renderTransfer(d) {
      const chart = $("transferChart");
      if (!chart) return;
      const tr = transferModel(d, now());
      if (!tr) {
        $("transferStamp").textContent = sourceIssue(d, "egress");
        setBadge("transferBadge", "unknown", "NO DATA");
        for (const id of ["transferValue", "transferQuota", "meterMid", "meterMax", "keyUsed", "keyProjected"]) $(id).textContent = "\u2014";
        $("transferMonth").textContent = "Not confirmed";
        $("transferDaysNote").textContent = "";
        $("meterUsed").style.width = "0%";
        Object.assign($("meterProjected").style, { left: "0%", width: "0%" });
        $("transferMeter").setAttribute("aria-label", "Data transfer measurement unavailable");
        clear($("transferFacts"));
        clear(chart).append(el("p", { class: "empty" }, "Data transfer measurement unavailable."));
        return;
      }
      const { v, fresh } = tr, q = v.quota_bytes;
      $("transferStamp").textContent = fresh ? `Measured ${jst(tr.observed)} JST \xB7 ${ago(tr.observed)}` : `Updates stopped \xB7 last ${jst(tr.observed, { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" })} JST`;
      $("transferMonth").textContent = new Date(tr.start * 1e3).toLocaleString("en-GB", { timeZone: "UTC", month: "long", year: "numeric" });
      const [state, label] = !fresh ? ["unknown", "NOT CURRENT"] : tr.usedRatio >= 1 ? ["critical", "OVER ALLOWANCE"] : tr.projHiRatio >= 1 ? ["serious", "ON PACE TO EXCEED"] : tr.projHiRatio >= 0.8 ? ["warning", "NEARING ALLOWANCE"] : ["good", "WITHIN ALLOWANCE"];
      setBadge("transferBadge", state, label);
      $("transferValue").textContent = bytes(v.month_egress_bytes);
      $("transferQuota").textContent = roundBytes(q);
      $("meterMid").textContent = roundBytes(q / 2);
      $("meterMax").textContent = roundBytes(q);
      const share2 = (r) => `${(100 * r).toFixed(r < 0.1 ? 1 : 0)}%`;
      const used = Math.min(1, tr.usedRatio), proj = Math.min(1, Math.max(used, tr.projHiRatio));
      $("meterUsed").style.width = `${used * 100}%`;
      Object.assign($("meterProjected").style, { left: `${used * 100}%`, width: `${(proj - used) * 100}%` });
      const projText = `${share2(tr.projLo / q)}\u2013${share2(tr.projHiRatio)}`;
      $("keyUsed").textContent = `used ${share2(tr.usedRatio)}`;
      $("keyProjected").textContent = `projected ${projText} by month end`;
      $("transferMeter").setAttribute("aria-label", `${share2(tr.usedRatio)} of the ${roundBytes(q)} allowance used; ${projText} projected by month end`);
      facts("transferFacts", [
        ["Last 24 hours", bytes(v.trailing_24h_egress_bytes)],
        ["7-day average", `${bytes(v.trailing_7d_avg_daily_egress_bytes)} / day`],
        ["Month-end projection", `${bytes(tr.projLo)} \u2013 ${bytes(tr.projHi)}`],
        ["Resets", `${jst(tr.end, { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" }).replace(",", "")} JST${untilDays(tr.end) ? ` (${untilDays(tr.end)})` : ""}`]
      ]);
      drawTransferDays(chart, tr);
    }
    function drawTransferDays(chart, tr) {
      clear(chart);
      const { v } = tr, nDays = Math.round((tr.end - tr.start) / 86400), pace = v.quota_bytes / nDays;
      const values = new Map((v.days || []).map((r) => [r.date, r.egress_bytes]));
      const today = new Date(now() * 1e3).toISOString().slice(0, 10);
      const days = Array.from({ length: nDays }, (_, i) => {
        const date = new Date((tr.start + i * 86400) * 1e3).toISOString().slice(0, 10);
        return { date, n: i + 1, value: values.get(date), partial: date === today };
      });
      $("transferDaysNote").textContent = `\xB7 ${values.size} of ${nDays} days measured`;
      const W = Math.max(280, Math.round(chart.clientWidth || 560)), H = 190, top = 18, base = H - 22;
      const max = Math.max(pace * 1.12, ...days.map((r) => r.value).filter(finite).map((x) => x * 1.12));
      const y = (val) => base - (base - top) * val / max;
      const col = W / nDays, bw = Math.max(3, col - 2);
      const svg = el("svg:svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": `Outbound data per UTC day; the dashed line is the ${bytes(pace)} per day pace that would use the full allowance` });
      svg.append(el("svg:line", { x1: 0, x2: W, y1: base, y2: base, class: "gridline" }));
      const complete = days.filter((r) => finite(r.value) && !r.partial);
      const latest = complete[complete.length - 1];
      const peak = complete.reduce((a, r) => !a || r.value > a.value ? r : a, null);
      const labelled = new Set([latest, peak && latest && Math.abs(peak.n - latest.n) * col >= 56 ? peak : null].filter(Boolean));
      for (const r of days) {
        const x = (r.n - 1) * col, g = el("svg:g", { class: "bar-col" });
        if (finite(r.value)) {
          const h = Math.max(base - y(r.value), r.value > 0 ? 2 : 0);
          g.append(el("svg:rect", { x: x + (col - bw) / 2, y: base - h, width: bw, height: h, rx: Math.min(4, bw / 2), class: `bar${r.partial ? " partial" : ""}` }));
          if (labelled.has(r)) g.append(el("svg:text", { x: Math.min(W - 20, Math.max(20, x + col / 2)), y: base - h - 5, "text-anchor": "middle", class: "bar-value" }, bytes(r.value)));
        }
        const text = `${r.date} UTC \xB7 ${finite(r.value) ? bytes(r.value) + (r.partial ? " so far" : "") : "no data"}`;
        const hit = el("svg:rect", { x, y: 0, width: col, height: base, class: "hit" });
        hit.addEventListener("mousemove", (e) => showTip(e, text));
        hit.addEventListener("mouseleave", hideTip);
        g.append(hit);
        if (r.n === 1 || r.n % 5 === 0) g.append(el("svg:text", { x: x + col / 2, y: H - 6, "text-anchor": "middle", class: "axis-label" }, r.n === 1 ? `${r.date.slice(5, 7)}/01` : String(r.n)));
        svg.append(g);
      }
      svg.append(
        el("svg:line", { x1: 0, x2: W, y1: y(pace), y2: y(pace), class: "ref" }),
        el("svg:text", { x: W, y: y(pace) - 5, "text-anchor": "end", class: "pace-label" }, `full-allowance pace \xB7 ${bytes(pace)} / day`)
      );
      chart.append(svg);
    }
    function formatItem(i) {
      if (!finite(i?.value)) return "n/a";
      if (i.id === "maintenance_active") return i.value > 0 ? "active" : "inactive";
      if (i.id === "subsystems_degraded") return `${Math.round(i.value)} reported degraded`;
      if (i.unit === "flag") return i.value > 0 ? "issue" : "clear";
      if (i.unit === "sec") return `${Math.round(i.value)} s`;
      if (i.unit === "Mbps") return `${i.value.toFixed(2)} Mbps`;
      if (i.unit === "count") return String(Math.round(i.value));
      return `${i.value} ${i.unit || ""}`.trim();
    }
    function renderChecks(d) {
      const groups = /* @__PURE__ */ new Map();
      for (const i of d.prom?.items || []) {
        if (!groups.has(i.group)) groups.set(i.group, []);
        groups.get(i.group).push(i);
      }
      const root = $("checkGroups");
      const expanded = new Map([...root.querySelectorAll("details[data-check]")].map(row => [row.dataset.check, row.open]));
      clear(root);
      let ok = 0, total = 0;
      for (const [name, items] of groups) {
        const box = el("div");
        box.append(el("h3", {}, name));
        const ul = el("ul");
        for (const i of items) {
          total++;
          if (i.state === "ok") ok++;
          const st = i.state === "ok" ? "good" : i.state === "warn" ? "warning" : i.state === "bad" ? "serious" : "unknown";
          const li = el("li");
          const detail = el("details", { class: "system-check", "data-check": i.id });
          detail.open = expanded.has(i.id) ? expanded.get(i.id) : i.state !== "ok";
          const heading = el("summary");
          heading.append(el("span", { class: `state ${st}` }, i.label), el("span", { class: "check-value" }, formatItem(i)));
          detail.append(heading);
          detail.append(el("p", { class: "check-description" }, i.description || "Latest reported result for this check."));
          if (i.normal_condition) detail.append(el("p", { class: "check-reference" }, i.normal_condition));
          if (i.id === "subsystems_degraded") {
            const components = el("ul", { class: "component-list" });
            for (const component of i.components || []) {
              const state = component.state === "healthy" ? "good" : component.state === "degraded" || component.state === "failed" ? "warning" : "unknown";
              const row = el("li");
              row.append(el("span", {}, component.label), el("span", { class: `state ${state}` }, String(component.state).replaceAll("_", " ")));
              components.append(row);
            }
            detail.append(components);
            li.append(detail, el("p", { class: "check-context" }, i.detail_summary || "Component details are unavailable."));
          } else {
            li.append(detail);
          }
          ul.append(li);
        }
        box.append(ul);
        root.append(box);
      }
      $("checksSummary").textContent = total ? `${ok} of ${total} checks OK` : "unavailable";
    }
    let latestData = null, refreshing = false, renderedFreshness = null;
    const freshnessSignature = (d) => Object.keys(PUBLIC_SOURCES).map((key) => d.freshness[key].state).join("|");
    function renderLatest() {
      const d = snapshotView(latestData, now());
      renderedFreshness = freshnessSignature(d);
      const m = model(d);
      for (const [name, fn] of [
        ["status", () => renderStatus(m)],
        ["now", () => renderNow(m)],
        ["transfer", () => renderTransfer(d)],
        ["timeline", () => renderTimeline(m)],
        ["clocks", () => renderClocks(d)],
        ["reliability", () => renderReliability(d)],
        ["incidents", () => renderIncidents(d)],
        ["reconnects", () => renderReconnects(d)],
        ["checks", () => renderChecks(d)]
      ]) {
        try {
          fn();
        } catch (err) {
          console.error(`render ${name} failed`, err);
        }
      }
    }
    function updateAges() {
      if (!latestData) return;
      const d = snapshotView(latestData, now());
      if (freshnessSignature(d) !== renderedFreshness) {
        renderLatest();
        return;
      }
      const m = model(d);
      renderStatus(m);
      renderNow(m);
      const clock = d.freshness.clock, rel = d.freshness.rel;
      if (clock.state === "fresh") $("clockStamp").textContent = `Measured ${jst(clock.generatedAt)} JST \xB7 ${ago(clock.generatedAt)}`;
      if (rel.state === "fresh") $("relStamp").textContent = `Measured ${jst(rel.generatedAt)} JST \xB7 ${ago(rel.generatedAt)}`;
    }
    async function refresh() {
      if (refreshing) return;
      refreshing = true;
      try {
        latestData = await load();
      } finally {
        refreshing = false;
        renderLatest();
      }
    }
    $("refresh").addEventListener("click", refresh);
    let resizeTimer;
    window.addEventListener("resize", () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => {
        if (latestData) try {
          renderTransfer(snapshotView(latestData, now()));
        } catch (err) {
          console.error("render transfer failed", err);
        }
      }, 150);
    });
    refresh();
    startAutoReload();
    setInterval(refresh, 6e4);
    setInterval(updateAges, 15e3);
  })();
})();
