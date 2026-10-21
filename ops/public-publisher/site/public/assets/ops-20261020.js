/* Tokyo Flight Tracker · live operations page (2026-10-20).
   Reads only the published JSON snapshots. Every part renders "unknown" on its own when its data is missing or stale. */
"use strict";
(() => {
  const SOURCES = {
    prom: "stream-v3-prometheus.json", loki: "stream-v3-loki.json", rel: "reliability-indicators.json",
    relay: "relay-observation.json", twitch: "twitch-observation.json", tsli: "twitch-sli.json",
  };
  const FRESH_WARN = 180, FRESH_BAD = 300, RANK = {good: 0, unknown: 1, warning: 2, serious: 3, critical: 4};
  const $ = id => document.getElementById(id);
  const SVGNS = "http://www.w3.org/2000/svg";
  const el = (tag, attrs = {}, text) => {
    const n = tag.startsWith("svg:") ? document.createElementNS(SVGNS, tag.slice(4)) : document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) if (v !== undefined && v !== null) n.setAttribute(k, v);
    if (text !== undefined) n.textContent = text;
    return n;
  };
  const clear = n => { while (n.firstChild) n.removeChild(n.firstChild); return n; };
  const finite = v => typeof v === "number" && Number.isFinite(v);
  const now = () => Date.now() / 1000;
  const toSec = v => finite(v) ? v : (typeof v === "string" ? Date.parse(v) / 1000 : NaN);
  const jst = (sec, opts = {hour: "2-digit", minute: "2-digit"}) =>
    new Date(sec * 1000).toLocaleString("en-GB", {timeZone: "Asia/Tokyo", hour12: false, ...opts});
  const ago = sec => {
    if (!finite(sec)) return "unknown";
    const d = Math.max(0, now() - sec);
    if (d < 90) return `${Math.round(d)} s ago`;
    if (d < 5400) return `${Math.round(d / 60)} min ago`;
    const h = Math.floor(d / 3600), m = Math.round((d % 3600) / 60);
    return h < 48 ? `${h} h ${m} min ago` : `${Math.round(h / 24)} days ago`;
  };
  const until = sec => {
    const d = sec - now();
    if (!finite(d) || d < 0) return "";
    const h = Math.floor(d / 3600), m = Math.round((d % 3600) / 60);
    return h ? `in ${h} h ${m} min` : `in ${m} min`;
  };
  const mbps = v => finite(v) ? `${v.toFixed(2)} Mbps` : "not measured";
  const pct = (v, d = 2) => finite(v) ? `${v.toFixed(d)}%` : "—";
  const worst = states => states.reduce((a, b) => (RANK[b] ?? 1) > (RANK[a] ?? 1) ? b : a, "good");

  /* ---------- tooltip ---------- */
  const tip = $("tooltip");
  function showTip(evt, text) {
    tip.textContent = text; tip.hidden = false;
    const x = Math.min(window.innerWidth - tip.offsetWidth - 8, evt.clientX + 12);
    const y = Math.max(8, evt.clientY - tip.offsetHeight - 12);
    tip.style.left = `${x}px`; tip.style.top = `${y}px`;
  }
  const hideTip = () => { tip.hidden = true; };

  /* ---------- data ---------- */
  async function load() {
    const entries = await Promise.all(Object.entries(SOURCES).map(async ([key, file]) => {
      try {
        const r = await fetch(`./${file}?t=${Date.now()}`, {cache: "no-store"});
        return [key, r.ok ? await r.json() : null];
      } catch { return [key, null]; }
    }));
    return Object.fromEntries(entries);
  }

  function model(d) {
    const t = now();
    const promAge = t - toSec(d.prom?.generated_at);
    const items = new Map((d.prom?.items || []).map(i => [i.id, i]));
    const relayFresh = d.relay?.schema === "stream.relay_public.v1" && d.relay.status === "OBSERVED"
      && d.relay.source_fresh === true && t - toSec(d.relay.generated_at) <= FRESH_WARN;
    const fb = d.relay?.fallback;
    const fbKnown = fb && typeof fb.active === "boolean" && t - toSec(fb.observed_at) <= FRESH_WARN;
    const fallback = fbKnown ? fb.active : null;
    const input = relayFresh ? d.relay.input : null;
    const dest = relayFresh ? d.relay.destinations : {};
    // Live problem flags decide the YouTube state. current_fail_1h counts interruptions in the past hour only.
    const ytFlags = ["youtube_issue", "ingest_issue", "same_url_issue"].map(id => items.get(id)).filter(Boolean);
    const ytBad = ytFlags.filter(i => finite(i.value) && i.value > 0);
    const ytRecent = items.get("current_fail_1h");
    const twState = d.twitch?.observation?.provider_state || "UNKNOWN";
    const twFresh = d.twitch?.observation?.source_fresh === true;
    // Renewal comes from the relay keeper's own state; the clock window is only a fallback when the relay is unobserved.
    const daily = relayFresh ? d.relay.daily : null;
    const renewing = daily ? (daily.planned_break === true || ["WAIT_STOP", "WAIT_OFFLINE", "WAIT_START", "VERIFYING"].includes(daily.state))
      : inRenewalWindow(t);
    const relayHealthy = relayFresh && input?.status === "FLOWING" && dest?.youtube?.status === "FLOWING";
    const fallbackReady = fbKnown ? fb.ready === true : null;
    return {t, d, items, promAge, relayFresh, relayHealthy, fallback, fallbackReady, input, dest, ytFlags, ytBad, ytRecent, twState, twFresh, renewing};
  }

  /* ---------- status banner ---------- */
  function inRenewalWindow(sec) {
    const h = Number(jst(sec, {hour: "2-digit"})), m = Number(jst(sec, {minute: "2-digit"}));
    return h === 0 && m < 15;
  }
  function assess(m) {
    const reasons = [];
    const add = (state, text) => reasons.push({state, text});
    if (!finite(m.promAge)) add("critical", "Monitoring snapshot is unavailable.");
    else if (m.promAge > FRESH_BAD) add("critical", `Monitoring snapshot is ${Math.round(m.promAge / 60)} min old.`);
    else if (m.promAge > FRESH_WARN) add("warning", `Monitoring snapshot is ${Math.round(m.promAge / 60)} min old.`);
    if (!m.ytFlags.length) add("warning", "YouTube checks are unavailable.");
    for (const i of m.ytBad) add("critical", `YouTube: ${i.label} (${formatItem(i)}).`);
    if (finite(m.ytRecent?.value) && m.ytRecent.value > 0)
      add("warning", `YouTube had ${m.ytRecent.value} interruption${m.ytRecent.value > 1 ? "s" : ""} in the last hour; it is live now.`);
    if (m.fallback === true && m.relayHealthy)
      add("warning", "The relay has recovered. The direct YouTube fallback keeps running until the relay has been healthy for 5 minutes, so two uploads run until then.");
    else if (m.fallback === true)
      add("serious", m.fallbackReady === false ? "Relay path is down. The direct YouTube fallback is starting."
        : "Relay path is down. YouTube continues on the automatic direct fallback; Twitch waits for the relay.");
    else if (m.relayFresh && m.input?.status !== "FLOWING") add("serious", "The relay is not receiving the upload from Dell.");
    else if (!m.relayFresh) add("warning", "Relay observation is unavailable.");
    if (m.relayFresh && m.input?.status === "FLOWING") {
      for (const [name, label] of [["youtube", "YouTube"], ["twitch", "Twitch"]])
        if (m.dest?.[name]?.status !== "FLOWING") {
          if (name === "twitch" && m.renewing) { if (m.twState !== "OFFLINE") add("warning", "Twitch daily session renewal is in progress; the relay output to Twitch is paused."); }
          else add("serious", `Relay output to ${label} is not progressing.`);
        }
    }
    if (m.twState === "OFFLINE") add(m.renewing ? "warning" : "serious",
      m.renewing ? "Twitch daily session renewal is in progress." : "Twitch is offline.");
    else if (m.twState !== "LIVE" || !m.twFresh) add("warning", "Twitch status is not confirmed.");
    // The banner reflects the live state. Warning-level budget alerts are listed under Incidents only.
    const critical = (m.d.rel?.active_incidents?.items || []).filter(i => String(i.severity).toLowerCase() === "critical");
    if (critical.length) add("warning", `${critical.length} critical reliability alert${critical.length > 1 ? "s" : ""} (see Incidents).`);
    const decision = new Set(["youtube_issue", "ingest_issue", "same_url_issue", "current_fail_1h", "ffmpeg_clusters_1h"]);
    for (const i of m.items.values()) if (!decision.has(i.id) && ["bad", "warn"].includes(i.state)) add("warning", `${i.label}: ${formatItem(i)}.`);
    const state = worst(reasons.map(r => r.state));
    return {state, reasons};
  }
  function renderStatus(m) {
    const {state, reasons} = assess(m);
    const title = {good: "All systems normal", warning: "Running · needs attention", serious: "Degraded", critical: "Disrupted"}[state];
    const icon = {good: "✓", warning: "!", serious: "!", critical: "×"}[state];
    $("status").className = `status-banner state-${state}`;
    $("statusIcon").textContent = icon;
    $("statusTitle").textContent = title;
    const list = clear($("statusReasons"));
    if (!reasons.length) list.append(el("li", {}, "YouTube and Twitch are live through the relay with a single upload."));
    for (const r of reasons.sort((a, b) => RANK[b.state] - RANK[a.state]).slice(0, 4)) list.append(el("li", {}, r.text));
    $("statusUpdated").textContent = finite(m.promAge) ? `Updated ${ago(toSec(m.d.prom.generated_at))}` : "No snapshot";
  }

  /* ---------- now ---------- */
  function setBadge(id, state, text) { const b = $(id); b.dataset.state = state; b.textContent = text; }
  function facts(id, rows) {
    const dl = clear($(id));
    for (const [k, v] of rows) if (v !== undefined && v !== null && v !== "") dl.append(el("dt", {}, k), el("dd", {}, v));
  }
  function routeName(m) {
    if (m.fallback === true && m.relayHealthy) return "OCI relay + direct fallback (stopping)";
    if (m.fallback === true) return "Direct fallback (relay down)";
    if (m.relayFresh && m.input?.status === "FLOWING") return "OCI relay";
    return "not confirmed";
  }
  function renderNow(m) {
    const ytOk = m.ytFlags.length && !m.ytBad.length;
    setBadge("ytBadge", !m.ytFlags.length ? "unknown" : ytOk ? "good" : "critical", !m.ytFlags.length ? "UNKNOWN" : ytOk ? "LIVE" : "ISSUE");
    $("ytValue").textContent = !m.ytFlags.length ? "Unknown" : ytOk ? "Live" : "Problem detected";
    const up = m.items.get("upload_p95_1h");
    facts("ytFacts", [
      ["Delivered via", routeName(m)],
      ["Relay output", m.relayFresh ? mbps(m.dest?.youtube?.mbps) : "not observed"],
      ["Upload p95 (1 h)", up ? formatItem(up) : null],
      ["Stream URL", m.items.get("same_url_issue")?.value === 0 ? "unchanged" : m.items.get("same_url_issue") ? "check" : null],
      ["Interruptions (1 h)", m.ytRecent ? formatItem(m.ytRecent) : null],
    ]);

    const tw = m.d.twitch;
    const twGood = m.twState === "LIVE" && m.twFresh;
    setBadge("twBadge", twGood ? "good" : m.twState === "OFFLINE" ? (m.renewing ? "warning" : "serious") : "unknown",
      twGood ? "LIVE" : m.twState === "OFFLINE" ? (m.renewing ? "RENEWING" : "OFFLINE") : "UNKNOWN");
    $("twValue").textContent = twGood ? "Live" : m.twState === "OFFLINE" ? "Offline" : "Unknown";
    const next = toSec(m.d.relay?.daily?.next_renewal_at);
    facts("twFacts", [
      ["Session started", tw?.session?.started_at ? ago(toSec(tw.session.started_at)) : null],
      ["Next renewal", finite(next) ? `00:00 JST, ${until(next)}` : "daily at 00:00 JST"],
      ["Relay output", m.relayFresh ? mbps(m.dest?.twitch?.mbps) : "not observed"],
      ["Viewers", finite(tw?.session?.viewer_count) ? String(tw.session.viewer_count) : null],
    ]);

    const vm = m.relayFresh ? m.d.relay.vm : null;
    if (m.fallback === true) {
      setBadge("routeBadge", m.relayHealthy ? "warning" : "serious", m.relayHealthy ? "FALLBACK ENDING" : "FALLBACK");
      const fbRate = m.d.relay?.fallback?.mbps, mainRate = m.relayFresh ? m.input?.mbps : null;
      $("routeValue").textContent = finite(fbRate) && finite(mainRate) ? `2 uploads · ${(fbRate + mainRate).toFixed(2)} Mbps`
        : finite(fbRate) ? `2 uploads · direct ${fbRate.toFixed(2)} Mbps` : "2 uploads";
      $("routeNote").textContent = m.relayHealthy ? "The relay is healthy again; the direct feed stops after 5 minutes of relay health."
        : "Dell is sending directly to YouTube until the relay is healthy for 5 minutes.";
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
      ["Relay", m.relayFresh ? `${m.d.relay.relay?.status === "RUNNING" ? "running" : "stopped"} · ${m.d.relay.relay?.restart_count ?? "?"} restarts` : "not observed"],
      ["Direct fallback", m.fallback === true ? `active · ${mbps(m.d.relay?.fallback?.mbps)}` : m.fallback === false ? "standby" : "unknown"],
      ["OCI CPU / memory", vm ? `${pct(vm.cpu_percent, 0)} / ${pct(vm.memory_percent, 0)}` : null],
      ["OCI disk free", vm ? pct(vm.disk_free_percent, 0) : null],
    ]);
    renderPath(m);
  }

  function renderPath(m) {
    const svg = $("pathSvg");
    [...svg.querySelectorAll("g,path,text")].forEach(n => n.remove());
    const linkState = s => !m.relayFresh ? "unknown" : s === "FLOWING" ? "good" : "critical";
    const add = (d, cls) => svg.append(el("svg:path", {d, class: `link ${cls}`}));
    add("M170 113 H300", linkState(m.input?.status));
    add("M450 113 C515 113 515 48 580 48", linkState(m.dest?.youtube?.status));
    add("M450 113 C515 113 515 178 580 178", linkState(m.dest?.twitch?.status));
    add("M95 85 C95 18 420 6 580 30", m.fallback === true ? "active" : "standby");
    const node = (x, y, title, sub) => {
      const g = el("svg:g", {class: "node"});
      g.append(el("svg:rect", {x, y, width: 150, height: 56, rx: 10}), el("svg:text", {x: x + 14, y: y + 24}, title), el("svg:text", {x: x + 14, y: y + 43, class: "sub"}, sub));
      svg.append(g);
    };
    node(20, 85, "Dell", "encoder + sender");
    node(300, 85, "OCI relay", m.relayFresh ? (m.d.relay.relay?.status === "RUNNING" ? "running" : "stopped") : "not observed");
    node(580, 20, "YouTube", "primary ingest");
    node(580, 150, "Twitch", "daily session");
    const label = (x, y, text, anchor = "middle") => svg.append(el("svg:text", {x, y, class: "link-label", "text-anchor": anchor}, text));
    const rate = s => m.relayFresh ? mbps(s?.mbps) : "not observed";
    label(235, 105, rate(m.input));
    label(500, 70, rate(m.dest?.youtube), "end");
    label(500, 170, rate(m.dest?.twitch), "end");
    label(330, 22, m.fallback === true ? `direct fallback · ${mbps(m.d.relay?.fallback?.mbps)}` : "direct fallback · standby");
  }

  /* ---------- 24 hours ---------- */
  function renderTimeline(m) {
    const root = clear($("timeline"));
    const t1 = m.t, t0 = t1 - 86400, W = 1000;
    const x = s => ((s - t0) / (t1 - t0)) * W;

    const lane = (label, sub, value, build, short = false) => {
      const row = el("div", {class: "lane"});
      const lab = el("div", {class: "lane-label"}, label); lab.append(el("small", {}, sub));
      const svg = el("svg:svg", {viewBox: `0 0 ${W} ${short ? 22 : 44}`, preserveAspectRatio: "none", class: short ? "short" : "", role: "img", "aria-label": `${label}, last 24 hours`});
      build(svg, short ? 22 : 44);
      row.append(lab, svg, el("div", {class: "lane-value"}, value));
      root.append(row);
    };

    // Twitch LIVE by hour
    const tl = (m.d.tsli?.timeline || []).map(b => ({...b, s: toSec(b.start_at)})).filter(b => finite(b.s) && b.s + 3600 > t0);
    let good = 0, bad = 0;
    for (const b of tl) { good += b.good || 0; bad += b.bad || 0; }
    lane("Twitch live", "by hour", good + bad ? pct(100 * good / (good + bad), 1) : "—", (svg, h) => {
      for (const b of tl) {
        const known = (b.good || 0) + (b.bad || 0);
        const st = !known ? "unknown" : b.bad === 0 ? "good" : b.bad > 30 ? "critical" : "warning";
        const r = el("svg:rect", {x: Math.max(0, x(b.s)) + 1, y: 3, width: Math.max(2, x(b.s + 3600) - Math.max(0, x(b.s)) - 2), height: h - 6, class: `cell ${st}`});
        const text = `${jst(b.s)}–${jst(b.s + 3600)} JST · live ${b.good || 0} min · offline ${b.bad || 0} min${b.unknown || b.missing ? ` · no data ${(b.unknown || 0) + (b.missing || 0)} min` : ""}`;
        r.addEventListener("mousemove", e => showTip(e, text)); r.addEventListener("mouseleave", hideTip);
        svg.append(r);
      }
    }, true);

    // Upload p95 with 5 Mbps ceiling
    const trend = id => ((m.d.prom?.trends || []).find(tr => tr.id === id)?.points || []).filter(p => finite(p.ts) && p.ts >= t0);
    const up = trend("upload_p95_24h");
    const latest = up.length ? up[up.length - 1].value : null;
    lane("Upload bitrate", "p95 · dashed = 5 Mbps ceiling", finite(latest) ? `${latest.toFixed(2)} Mbps` : "—", (svg, h) => {
      const vals = up.map(p => p.value).filter(finite);
      const vmax = Math.max(5.5, ...vals), vmin = Math.max(0, Math.min(4, ...vals) - 0.5);
      const y = v => h - 3 - ((v - vmin) / (vmax - vmin)) * (h - 8);
      svg.append(el("svg:line", {x1: 0, x2: W, y1: y(vmin), y2: y(vmin), class: "gridline"}));
      svg.append(el("svg:line", {x1: 0, x2: W, y1: y(5), y2: y(5), class: "ref"}));
      let d = "", pen = false;
      for (const p of up) {
        if (!finite(p.value)) { pen = false; continue; }
        d += `${pen ? "L" : "M"}${x(p.ts).toFixed(1)} ${y(p.value).toFixed(1)}`; pen = true;
      }
      svg.append(el("svg:path", {d, class: "line", "vector-effect": "non-scaling-stroke"}));
      crosshair(svg, h, up, x, p => `${jst(p.ts)} JST · upload p95 ${finite(p.value) ? p.value.toFixed(2) + " Mbps" : "no data"}`);
    });

    // Encoder restarts derived from FFmpeg uptime resets
    const upt = trend("ffmpeg_uptime_24h");
    const restarts = [];
    for (let i = 1; i < upt.length; i++) if (finite(upt[i].value) && finite(upt[i - 1].value) && upt[i].value < upt[i - 1].value) restarts.push(upt[i]);
    lane("Encoder restarts", "detected from uptime resets", String(restarts.length), (svg, h) => {
      svg.append(el("svg:line", {x1: 0, x2: W, y1: h / 2, y2: h / 2, class: "gridline"}));
      for (const p of restarts) {
        const start = p.ts - p.value;
        const g = el("svg:g");
        g.append(el("svg:line", {x1: x(start), x2: x(start), y1: 3, y2: h - 3, class: "tick", "vector-effect": "non-scaling-stroke"}));
        const hit = el("svg:rect", {x: x(start) - 8, y: 0, width: 16, height: h, class: "hit"});
        hit.addEventListener("mousemove", e => showTip(e, `Encoder (re)started around ${jst(start)} JST`)); hit.addEventListener("mouseleave", hideTip);
        g.append(hit); svg.append(g);
      }
    }, true);

    // shared time axis
    // HTML labels so the text is not stretched with the lanes.
    const axis = el("div", {class: "axis-row"});
    const ticks = el("div", {class: "axis-ticks"});
    const firstHour = Math.ceil(t0 / 10800) * 10800;
    for (let s = firstHour; s <= t1; s += 10800) {
      const span = el("span", {}, jst(s, {hour: "2-digit", minute: "2-digit"}));
      span.style.left = `${(x(s) / W) * 100}%`;
      ticks.append(span);
    }
    axis.append(el("div"), ticks, el("div"));
    root.append(axis);
  }
  function crosshair(svg, h, pts, x, text) {
    if (!pts.length) return;
    const cursor = el("svg:line", {y1: 0, y2: h, class: "cursor", visibility: "hidden", "vector-effect": "non-scaling-stroke"});
    const hit = el("svg:rect", {x: 0, y: 0, width: 1000, height: h, class: "hit"});
    hit.addEventListener("mousemove", e => {
      const box = svg.getBoundingClientRect();
      const px = ((e.clientX - box.left) / box.width) * 1000;
      let best = pts[0];
      for (const p of pts) if (Math.abs(x(p.ts) - px) < Math.abs(x(best.ts) - px)) best = p;
      cursor.setAttribute("x1", x(best.ts)); cursor.setAttribute("x2", x(best.ts)); cursor.setAttribute("visibility", "visible");
      showTip(e, text(best));
    });
    hit.addEventListener("mouseleave", () => { cursor.setAttribute("visibility", "hidden"); hideTip(); });
    svg.append(cursor, hit);
  }

  /* ---------- reliability ---------- */
  const factValue = (f) => finite(f.value) ? `${Number.isInteger(f.value) ? f.value.toLocaleString("en-US") : f.value.toLocaleString("en-US", {maximumFractionDigits: 3})}${f.unit && f.unit !== "%" ? " " + f.unit : f.unit || ""}` : String(f.value ?? "—");
  function relRows(d) {
    const rows = [];
    for (const it of d.rel?.items || []) {
      const ref = (it.facts || []).find(f => /^reference$/i.test(f.label));
      const target = ref && finite(ref.value) ? ref.value : null;
      let state = "unknown", label = "no target";
      if (it.unit === "%" && finite(it.value) && target !== null) {
        state = it.value >= target ? "good" : it.value >= target - 1 ? "warning" : "serious";
        label = it.value >= target ? "meets target" : `${(target - it.value).toFixed(2)} pt below`;
      } else if (it.id === "same_url_observation") {
        state = it.value === 0 ? "good" : "warning"; label = it.value === 0 ? "URL unchanged" : "URL changed";
      }
      rows.push({name: it.label, window: it.window, value: it.unit === "%" ? pct(it.value, 2) : `${factValue({value: it.value})} ${it.unit || ""}`,
        pctValue: it.unit === "%" ? it.value : null, recent: finite(it.recent_24h) ? it.recent_24h : null, target, state, label, facts: it.facts || [], note: it.note});
    }
    const week = (d.tsli?.windows || []).find(w => w.id === "7d");
    const day = (d.tsli?.windows || []).find(w => w.id === "24h");
    for (const mt of week?.metrics || []) {
      if (!["provider_live", "daily_renewal"].includes(mt.id)) continue;
      const collecting = mt.assessment === "COLLECTING";
      const noResult = !finite(mt.value_percent);
      const ok = finite(mt.value_percent) && finite(mt.target_percent) && mt.value_percent >= mt.target_percent;
      const recent = (day?.metrics || []).find(x => x.id === mt.id)?.value_percent;
      rows.push({name: mt.label, window: "rolling 7d", value: pct(mt.value_percent, 1), pctValue: mt.value_percent,
        recent: mt.id === "provider_live" && finite(recent) ? recent : null, target: mt.target_percent,
        sub: mt.id === "daily_renewal" && finite(mt.expected_events)
          ? `${mt.good_events ?? 0} ok · ${mt.bad_events ?? 0} failed · ${mt.unknown_events ?? 0} unknown` : null,
        state: collecting || noResult ? "unknown" : ok ? "good" : "serious",
        label: noResult ? "no result yet" : collecting ? (finite(mt.coverage_percent) ? `collecting data (coverage ${pct(mt.coverage_percent, 0)})` : "collecting data") : ok ? "meets target" : "below target",
        facts: Object.entries(mt).filter(([k, v]) => /minutes$|events$|percent$/.test(k) && finite(v)).map(([k, v]) => ({label: k.replaceAll("_", " "), value: v})),
        note: mt.definition});
    }
    return rows;
  }
  function renderReliability(d) {
    const body = clear($("relRows"));
    const gen = toSec(d.rel?.generated_at);
    $("relStamp").textContent = finite(gen) ? `Measured ${jst(gen)} JST · ${ago(gen)}` : "Measurement unavailable";
    const rows = relRows(d);
    if (!rows.length) { const tr = el("tr"); tr.append(el("td", {colspan: 6, class: "empty"}, "No reliability data published.")); body.append(tr); return; }
    for (const r of rows) {
      const tr = el("tr", {class: "row", tabindex: "0", "aria-expanded": "false"});
      const valueCell = el("td", {class: "num"}, r.value);
      // The 7-day value only moves when old samples leave the window; the 24-hour value shows recent recovery.
      if (finite(r.recent)) valueCell.append(el("small", {class: "recent"}, `24 h ${pct(r.recent, r.recent % 1 ? 1 : 0)}`));
      if (r.sub) valueCell.append(el("small", {class: "recent"}, r.sub));
      tr.append(el("td", {}, r.name), el("td", {class: "window"}, r.window || ""), valueCell,
        el("td", {class: "num"}, finite(r.target) ? pct(r.target, r.target % 1 ? 1 : 0) : "—"));
      const barCell = el("td");
      if (finite(r.pctValue) && finite(r.target)) {
        // Start at 90% so sub-point gaps stay visible, but lower the start (in 10-point steps) when a value is below it,
        // otherwise low values all sit at the left edge and never appear to move.
        const lowest = Math.min(r.pctValue, r.target, finite(r.recent) ? r.recent : 100);
        // One 10-point step below the lowest value, so a value never sits exactly at the left edge.
        const lo = lowest >= 90.5 ? 90 : Math.max(0, Math.floor((lowest - 0.5) / 10) * 10);
        const pos = v => `${Math.max(0, Math.min(100, ((v - lo) / (100 - lo)) * 100))}%`;
        const scale = `scale ${lo}–100%`;
        const bar = el("div", {class: "gap-bar", title: `${r.value} (7 d)${finite(r.recent) ? `, ${pct(r.recent, 1)} (24 h)` : ""} vs ${pct(r.target, 1)} target · ${scale}`});
        const fill = el("i"); fill.style.width = pos(r.pctValue);
        const mark = el("b"); mark.style.left = pos(r.target);
        bar.append(fill, mark);
        if (finite(r.recent)) { const dot = el("u", {title: `Last 24 hours: ${pct(r.recent, 2)}`}); dot.style.left = pos(r.recent); bar.append(dot); }
        barCell.append(bar, el("small", {class: "scale"}, scale));
      }
      tr.append(barCell, (() => {
        const td = el("td"); td.append(el("span", {class: `state ${r.state}`}, r.label));
        // The 7-day status can hide a worse last day; say so beside it.
        if (finite(r.recent) && finite(r.target) && finite(r.pctValue) && r.recent < r.target && r.recent < r.pctValue)
          td.append(el("small", {class: "recent"}, `24 h ${(r.target - r.recent).toFixed(2)} pt below`));
        return td;
      })());
      const det = el("tr", {class: "details", hidden: ""});
      const td = el("td", {colspan: 6});
      const dl = el("dl");
      for (const f of r.facts) { const div = el("div"); div.append(el("dt", {}, f.label), el("dd", {}, factValue(f))); dl.append(div); }
      td.append(dl); if (r.note) td.append(el("p", {}, r.note));
      det.append(td);
      const toggle = () => { det.hidden = !det.hidden; tr.setAttribute("aria-expanded", String(!det.hidden)); };
      tr.addEventListener("click", toggle);
      tr.addEventListener("keydown", e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(); } });
      body.append(tr, det);
    }
  }

  /* ---------- incidents & recovery ---------- */
  const INCIDENTS = {
    "reliability:youtube_availability_multi_window_burn": ["YouTube availability below target", "Delivery gaps used error budget faster than allowed in both short and long windows."],
    "reliability:youtube_input_quality_multi_window_burn": ["YouTube input quality below target", "YouTube reported ingest health problems more often than allowed."],
    "reliability:audio_correctness_multi_window_burn": ["Audio checks below target", "Audio checks failed more often than allowed in both short and long windows."],
    "reliability:upload_ceiling_coverage_or_freshness": ["Upload measurement gap", "Upload bitrate measurements were missing or stale for part of the window."],
    "external:blackbox_failed": ["External uptime check failed", "Independent cloud checkers could not confirm this page or the YouTube video from outside the home network."],
  };
  function renderIncidents(d) {
    const list = clear($("incidentList"));
    const proj = d.rel?.active_incidents;
    if (!proj || proj.status !== "ok") { list.append(el("li", {class: "empty"}, "Alert state unavailable.")); }
    else if (!(proj.items || []).length) { list.append(el("li", {class: "empty"}, "No active alerts.")); }
    for (const inc of proj?.items || []) {
      const [name, why] = INCIDENTS[inc.id] || [String(inc.id || "Alert").replace(/^reliability:/, "").replaceAll("_", " "), ""];
      const sev = String(inc.severity || "unknown").toLowerCase();
      const state = sev === "critical" ? "critical" : sev === "warning" ? "warning" : "unknown";
      const li = el("li");
      li.append(el("span", {class: `state ${state}`}, sev), el("b", {title: inc.id || ""}, name));
      const seen = toSec(inc.observed_at);
      // Budget alerts come from an hourly evaluation of the past 1-6 hours, so they can outlive their cause by up to an hour.
      const burn = String(inc.id || "").endsWith("_multi_window_burn") ? "covers the past 1–6 h; may stay listed up to 1 h after the cause clears" : "";
      li.append(el("small", {}, [why, finite(seen) ? `evaluated ${jst(seen)} JST` : "", burn, inc.recovery_type === "human_review_no_automatic_restart" ? "operator review, no automatic restart" : ""].filter(Boolean).join(" · ")));
      list.append(li);
    }

    const act = d.loki?.recovery_activity;
    const daily = Object.entries(act?.daily_counts || {}).sort(([a], [b]) => a.localeCompare(b));
    const chart = clear($("recChart"));
    $("recTotal").textContent = act ? `· ${act.total_executed ?? "?"} in ${act.period?.days ?? 7} days` : "";
    $("recBreakdown").textContent = act?.action_counts
      ? Object.entries(act.action_counts).filter(([, v]) => v > 0).map(([k, v]) => `${k.replaceAll("_", " ")} ${v}`).join(" · ") + ". Counts include planned work such as route changes."
      : "Recovery activity unavailable.";
    if (!daily.length) { chart.append(el("p", {class: "empty"}, "No data.")); return; }
    const W = 420, H = 150, pad = 22, max = Math.max(1, ...daily.map(([, v]) => v));
    const svg = el("svg:svg", {viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": "Automatic recoveries per day"});
    const bw = (W / daily.length) * 0.62;
    daily.forEach(([day, v], i) => {
      const cx = (W / daily.length) * (i + 0.5);
      const h = ((H - pad - 16) * v) / max;
      const bar = el("svg:rect", {x: cx - bw / 2, y: H - pad - h, width: bw, height: Math.max(h, v ? 2 : 0), rx: 4, class: "bar"});
      bar.addEventListener("mousemove", e => showTip(e, `${day} · ${v} recoveries`)); bar.addEventListener("mouseleave", hideTip);
      svg.append(bar, el("svg:text", {x: cx, y: H - pad - h - 4, "text-anchor": "middle", class: "bar-value"}, String(v)),
        el("svg:text", {x: cx, y: H - 6, "text-anchor": "middle", class: "bar-label"}, day.slice(5).replace("-", "/")));
    });
    svg.append(el("svg:line", {x1: 0, x2: W, y1: H - pad, y2: H - pad, class: "gridline"}));
    chart.append(svg);
  }

  /* ---------- checks ---------- */
  function formatItem(i) {
    if (!finite(i?.value)) return "n/a";
    if (i.unit === "flag") return i.value > 0 ? "issue" : "clear";
    if (i.unit === "sec") return `${Math.round(i.value)} s`;
    if (i.unit === "Mbps") return `${i.value.toFixed(2)} Mbps`;
    if (i.unit === "count") return String(Math.round(i.value));
    return `${i.value} ${i.unit || ""}`.trim();
  }
  function renderChecks(d) {
    const groups = new Map();
    for (const i of d.prom?.items || []) { if (!groups.has(i.group)) groups.set(i.group, []); groups.get(i.group).push(i); }
    const root = clear($("checkGroups"));
    let ok = 0, total = 0;
    for (const [name, items] of groups) {
      const box = el("div"); box.append(el("h3", {}, name));
      const ul = el("ul");
      for (const i of items) {
        total++; if (i.state === "ok") ok++;
        const st = i.state === "ok" ? "good" : i.state === "warn" ? "warning" : i.state === "bad" ? "serious" : "unknown";
        const li = el("li"); li.append(el("span", {class: `state ${st}`}, i.label), el("span", {}, formatItem(i)));
        ul.append(li);
      }
      box.append(ul); root.append(box);
    }
    $("checksSummary").textContent = total ? `${ok} of ${total} checks OK` : "unavailable";
  }

  /* ---------- main ---------- */
  async function refresh() {
    const d = await load();
    const m = model(d);
    for (const [name, fn] of [["status", () => renderStatus(m)], ["now", () => renderNow(m)], ["timeline", () => renderTimeline(m)],
      ["reliability", () => renderReliability(d)], ["incidents", () => renderIncidents(d)], ["checks", () => renderChecks(d)]]) {
      try { fn(); } catch (err) { console.error(`render ${name} failed`, err); }
    }
  }
  $("refresh").addEventListener("click", refresh);
  refresh();
  setInterval(refresh, 60000);
})();
