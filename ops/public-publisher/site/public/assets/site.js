"use strict";

const ids = {
  snapshotLine: "snapshotLine",
  refresh: "refresh",
  overallState: "overallState",
  overallMeta: "overallMeta",
  snapshotState: "snapshotState",
  snapshotMeta: "snapshotMeta",
  youtubeState: "youtubeState",
  youtubeMeta: "youtubeMeta",
  mapState: "mapState",
  mapMeta: "mapMeta",
  autonomyState: "autonomyState",
  autonomyMeta: "autonomyMeta",
  metricCount: "metricCount",
  checkList: "checkList",
  reliabilityStamp: "reliabilityStamp",
  reliabilityGrid: "reliabilityGrid",
  guardGrid: "guardGrid",
  trendCount: "trendCount",
  trendGrid: "trendGrid",
  evidenceCount: "evidenceCount",
  evidenceGrid: "evidenceGrid",
  eventCount: "eventCount",
  eventList: "eventList",
};

const defaultFreshnessPolicy = {
  warn_after_sec: 180,
  bad_after_sec: 300,
};

const checkRows = [
  {
    title: "Public stream",
    subtitle: "same URL / YouTube watchdog / RTMPS ingest",
    metrics: ["current_fail_1h", "youtube_issue", "ingest_issue", "same_url_issue"],
  },
  {
    title: "Production map",
    subtitle: "monitor freshness / render / browser / NVENC / RTMP / precipitation",
    metrics: [
      "map_sample_age",
      "map_delivery_issue",
      "map_render_issue",
      "map_browser_issue",
      "map_nvenc_issue",
      "map_rtmp_issue",
      "map_weather_issue",
      "map_runtime_restarts",
    ],
  },
  {
    title: "Recovery loop",
    subtitle: "fast recovery / ffmpeg restart cluster / notify queue",
    metrics: ["restarts_1h", "ffmpeg_clusters_1h", "notify_issues"],
  },
  {
    title: "Observers",
    subtitle: "network observer / stream watchdog / subsystem coverage",
    metrics: ["network_issue", "watchdog_issue", "subsystems_degraded", "maintenance_active"],
  },
];

const guardRows = [
  {
    title: "Upload p95",
    metric: "upload_p95_1h",
    detail: "5 Mbps warn / 8 Mbps bad",
  },
  {
    title: "ADS-B evidence age",
    metric: "adsb_age",
    detail: "180s warn / 300s bad",
  },
  {
    title: "YouTube API units today (PT)",
    metric: "api_open_day_units",
    detail: "8000 warn / 9000 bad",
  },
  {
    title: "Memory guard",
    metric: "memory_guard_issue",
    detail: "0 is healthy",
  },
];

const eventKeys = [
  "level",
  "status",
  "stage",
  "mode",
  "action",
  "result",
  "reason",
  "judgment",
  "incident_reason",
  "failure_kind",
  "failure_subkind",
  "execute",
  "executable",
  "blocked_by",
  "ingest_connected",
  "stream_active",
  "public_ok",
  "healthy",
  "api_ok",
  "oauth_ok",
  "availability_ok",
  "local_ok",
  "maintenance_active",
  "ffmpeg_uptime_sec",
];

function byId(name) {
  return document.getElementById(ids[name]);
}

function cssState(state) {
  if (state === "ok") return "ok";
  if (state === "bad") return "bad";
  if (state === "warn") return "warn";
  return "unknown";
}

function stateText(state) {
  if (state === "ok") return "OK";
  if (state === "bad") return "needs action";
  if (state === "warn") return "watch";
  return "unknown";
}

function worstState(states) {
  if (states.includes("bad")) return "bad";
  if (states.includes("warn") || states.includes("unknown")) return "warn";
  return "ok";
}

function metricMap(prom) {
  const map = new Map();
  for (const item of prom?.items || []) map.set(item.id, item);
  return map;
}

function formatTime(ts) {
  const num = Number(ts);
  if (!Number.isFinite(num)) return "n/a";
  return new Date(num * 1000).toLocaleString("ja-JP", { hour12: false });
}

function formatDuration(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value)) return "n/a";
  if (value < 90) return `${Math.max(0, Math.round(value))}s`;
  if (value < 3600) return `${Math.round(value / 60)}m`;
  if (value < 86400) return `${(value / 3600).toFixed(value < 7200 ? 1 : 0)}h`;
  return `${(value / 86400).toFixed(1)}d`;
}

function formatScalar(value, unit) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "n/a";
  if (unit === "flag") return num <= 0 ? "clear" : "raised";
  if (unit === "sec") return formatDuration(num);
  if (unit === "Mbps") return `${num.toFixed(2)} Mbps`;
  if (unit === "units") return `${Math.round(num).toLocaleString("ja-JP")} units`;
  if (unit === "count") return `${Math.round(num).toLocaleString("ja-JP")} count`;
  if (Math.abs(num) >= 100) return num.toFixed(0);
  if (Math.abs(num) >= 10) return num.toFixed(1);
  return num.toFixed(2);
}

function formatValue(item) {
  if (!item || item.value === null || item.value === undefined) return "n/a";
  return formatScalar(item.value, item.unit || "");
}

function clearNode(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function setStateBlock(idBase, state, label, meta) {
  const stateEl = byId(`${idBase}State`);
  const metaEl = byId(`${idBase}Meta`);
  stateEl.textContent = label || stateText(state);
  stateEl.className = cssState(state);
  metaEl.textContent = meta;
}

function rowMetrics(map, row) {
  return row.metrics.map((metric) => map.get(metric)).filter(Boolean);
}

function mapRuntimeSummary(map) {
  const ids = [
    "map_sample_age",
    "map_delivery_issue",
    "map_render_issue",
    "map_browser_issue",
    "map_nvenc_issue",
    "map_rtmp_issue",
    "map_weather_issue",
    "map_runtime_restarts",
  ];
  const items = ids.map((id) => map.get(id)).filter(Boolean);
  const state = worstState(items.map((item) => item.state || "unknown"));
  const label = state === "ok" ? "READY" : state === "bad" ? "needs action" : "degraded";
  const details = items.map((item) => `${item.label}: ${formatValue(item)}`);
  return {
    state,
    label,
    meta: details.length ? details.join(" / ") : "map runtime metrics missing",
  };
}

function makeChip(item) {
  const chip = document.createElement("span");
  chip.className = `chip ${cssState(item?.state)}`;
  chip.textContent = `${item?.label || item?.id || "metric"}: ${formatValue(item)}`;
  return chip;
}

function renderChecks(map) {
  const target = byId("checkList");
  clearNode(target);

  for (const row of checkRows) {
    const metrics = rowMetrics(map, row);
    const state = worstState(metrics.map((item) => item.state || "unknown"));
    const card = document.createElement("article");
    card.className = `check-row ${cssState(state)}`;

    const head = document.createElement("div");
    head.className = "check-head";

    const titleWrap = document.createElement("div");
    const title = document.createElement("h3");
    title.textContent = row.title;
    const subtitle = document.createElement("p");
    subtitle.textContent = row.subtitle;
    titleWrap.append(title, subtitle);

    const badge = document.createElement("strong");
    badge.className = `state-badge ${cssState(state)}`;
    badge.textContent = stateText(state);
    head.append(titleWrap, badge);

    const chips = document.createElement("div");
    chips.className = "chips";
    for (const item of metrics) chips.appendChild(makeChip(item));

    card.append(head, chips);
    target.appendChild(card);
  }
}

function renderGuards(map) {
  const target = byId("guardGrid");
  clearNode(target);

  for (const row of guardRows) {
    const item = map.get(row.metric);
    const state = item?.state || "unknown";
    const card = document.createElement("article");
    card.className = `guard ${cssState(state)}`;

    const label = document.createElement("span");
    label.textContent = row.title;
    const value = document.createElement("strong");
    value.textContent = formatValue(item);
    const detail = document.createElement("small");
    detail.textContent = row.detail;

    card.append(label, value, detail);
    target.appendChild(card);
  }
}

function formatReliabilityValue(value, unit) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "n/a";
  if (unit === "%") return `${num.toFixed(3)}%`;
  if (unit === "pt") return `${num >= 0 ? "+" : ""}${num.toFixed(3)} pt`;
  if (unit === "Mbps") return `${num.toFixed(3)} Mbps`;
  if (unit === "min") return `${Math.round(num).toLocaleString("ja-JP")} min`;
  if (unit === "samples" || unit === "events" || unit === "transitions" || unit === "URLs") {
    return `${Math.round(num).toLocaleString("ja-JP")} ${unit}`;
  }
  return Number.isInteger(num) ? num.toLocaleString("ja-JP") : num.toFixed(3);
}

function renderReliability(payload) {
  const target = byId("reliabilityGrid");
  clearNode(target);

  const items = payload?.items || [];
  const generatedAt = Number(payload?.generated_at);
  if (Number.isFinite(generatedAt)) {
    const ageSec = Math.max(0, Date.now() / 1000 - generatedAt);
    byId("reliabilityStamp").textContent = `measured ${formatTime(generatedAt)} / ${formatDuration(ageSec)} old`;
  } else {
    byId("reliabilityStamp").textContent = "measurement unavailable";
  }

  if (!items.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No reliability measurement published";
    target.appendChild(empty);
    return;
  }

  for (const item of items) {
    const card = document.createElement("article");
    card.className = "reliability-card";

    const head = document.createElement("div");
    head.className = "reliability-head";
    const title = document.createElement("h3");
    title.textContent = item.label || item.id;
    const window = document.createElement("span");
    window.textContent = item.window || "observed";
    head.append(title, window);

    const value = document.createElement("strong");
    value.textContent = formatReliabilityValue(item.value, item.unit);

    const facts = document.createElement("dl");
    facts.className = "reliability-facts";
    for (const fact of item.facts || []) {
      const term = document.createElement("dt");
      term.textContent = fact.label;
      const detail = document.createElement("dd");
      detail.textContent = formatReliabilityValue(fact.value, fact.unit);
      facts.append(term, detail);
    }

    const note = document.createElement("p");
    note.className = "reliability-note";
    note.textContent = item.note || "Measured observation";

    card.append(head, value, facts, note);
    target.appendChild(card);
  }
}

function policyFrom(prom, loki) {
  return prom?.freshness_policy || loki?.freshness_policy || defaultFreshnessPolicy;
}

function snapshotFreshness(prom, loki) {
  const policy = policyFrom(prom, loki);
  const times = [prom?.generated_at, loki?.generated_at]
    .map(Number)
    .filter((value) => Number.isFinite(value) && value > 0);

  if (!times.length) {
    return {
      state: "bad",
      ageSec: null,
      label: "missing",
      meta: "no generated_at in public snapshots",
      policy,
    };
  }

  const oldest = Math.min(...times);
  const ageSec = Math.max(0, Date.now() / 1000 - oldest);
  const state = ageSec >= policy.bad_after_sec ? "bad" : ageSec >= policy.warn_after_sec ? "warn" : "ok";
  return {
    state,
    ageSec,
    oldest,
    label: `${formatDuration(ageSec)} old`,
    meta: `warn > ${formatDuration(policy.warn_after_sec)} / bad > ${formatDuration(policy.bad_after_sec)}`,
    policy,
  };
}

function sectionById(loki, id) {
  return (loki?.sections || []).find((section) => section.id === id) || null;
}

function sectionEvents(loki, id) {
  return sectionById(loki, id)?.events || [];
}

function eventTimestamp(event) {
  if (event.ts_utc) return event.ts_utc;
  if (!event.ts_ns) return "";
  const seconds = Number.parseInt(event.ts_ns, 10) / 1_000_000_000;
  if (!Number.isFinite(seconds)) return "";
  return new Date(seconds * 1000).toISOString().replace(".000", "");
}

function tryJson(value) {
  if (typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch (_error) {
    return null;
  }
}

function eventText(event) {
  return JSON.stringify(event || {}).toLowerCase();
}

function isExecutedEvent(event) {
  if (event?.execute === true) return true;
  const result = tryJson(event?.result);
  if (result?.status && String(result.status).toLowerCase() !== "not_executed") {
    return /execut|complete|restart|done|success/.test(String(result.status).toLowerCase());
  }
  return /"status"\s*:\s*"executed"/.test(eventText(event));
}

function isShadowEvent(event) {
  return event?.mode === "shadow" || /shadow_mode|mode..shadow/.test(eventText(event));
}

function isGatedEvent(event) {
  return /gates_block|blocked_by|shadow_mode|blocked/.test(eventText(event));
}

function boundaryWindow(loki, name) {
  return loki?.recovery_boundary?.windows?.[name] || {};
}

function productionActionCount(loki, windowName, action) {
  return Number(boundaryWindow(loki, windowName)?.production_action_counts?.[action] || 0);
}

function boundaryScalar(loki, windowName, key) {
  return Number(boundaryWindow(loki, windowName)?.[key] || 0);
}

function boundaryHasField(loki, windowName, key) {
  return Object.prototype.hasOwnProperty.call(boundaryWindow(loki, windowName), key);
}

function boundaryReasonCount(loki, windowName, reason) {
  return Number(boundaryWindow(loki, windowName)?.shadow_vs_production_disagreement_by_reason?.[reason] || 0);
}

function boundaryClassifierReplay(loki, windowName) {
  return boundaryWindow(loki, windowName)?.current_classifier_replay || {};
}

function boundaryInterpretation(loki) {
  return boundaryWindow(loki, "last_30d")?.interpretation
    || boundaryWindow(loki, "last_7d")?.interpretation
    || "";
}

function autonomySummary(loki) {
  const orchestrator = [
    ...sectionEvents(loki, "recovery_orchestrator_30d"),
    ...sectionEvents(loki, "recovery_orchestrator"),
  ];
  const plans = [
    ...sectionEvents(loki, "recovery_action_plan_30d"),
    ...sectionEvents(loki, "recovery_action_plan"),
  ];
  const executed = orchestrator.filter(isExecutedEvent);
  const shadow = orchestrator.filter(isShadowEvent);
  const gated = plans.filter(isGatedEvent);
  const latest = orchestrator[0] || plans[0] || null;
  const productionRestart30d = productionActionCount(loki, "last_30d", "restart_stream");
  const productionRestart7d = productionActionCount(loki, "last_7d", "restart_stream");
  const recoveryIntent30d = boundaryScalar(loki, "last_30d", "shadow_recovery_intent_action_count");
  const recoveryIntent7d = boundaryScalar(loki, "last_7d", "shadow_recovery_intent_action_count");
  const recoveryFalsePositive30d = boundaryReasonCount(loki, "last_30d", "false_positive_shadow");
  const recoveryFalsePositive7d = boundaryReasonCount(loki, "last_7d", "false_positive_shadow");
  const hasRecoveryIntentSli = boundaryHasField(loki, "last_7d", "shadow_recovery_intent_action_count");
  const classifierReplay30d = boundaryClassifierReplay(loki, "last_30d");
  const classifierReplay7d = boundaryClassifierReplay(loki, "last_7d");
  const classifierEligible30d = Number(classifierReplay30d.eligible_count || 0);
  const classifierCovered30d = Number(classifierReplay30d.covered_count || 0);
  const classifierUncovered30d = Number(classifierReplay30d.uncovered_count || 0);
  const classifierEligible7d = Number(classifierReplay7d.eligible_count || 0);
  const classifierCovered7d = Number(classifierReplay7d.covered_count || 0);
  const classifierUncovered7d = Number(classifierReplay7d.uncovered_count || 0);
  const hasClassifierReplay = boundaryHasField(loki, "last_7d", "current_classifier_replay");
  const recoveryIntentText = hasRecoveryIntentSli
    ? `executor recovery intent ${recoveryIntent30d} in 30d (${recoveryIntent7d} in 7d), false-positive intent ${recoveryFalsePositive30d} in 30d (${recoveryFalsePositive7d} in 7d)`
    : "executor recovery intent SLI pending";
  const classifierReplayText = hasClassifierReplay
    ? `current classifier replay ${classifierCovered30d}/${classifierEligible30d} in 30d (${classifierCovered7d}/${classifierEligible7d} in 7d), uncovered ${classifierUncovered30d} in 30d (${classifierUncovered7d} in 7d)`
    : "current classifier replay pending";
  const ownerText = productionRestart30d
    ? `production restart_stream ${productionRestart30d} in 30d (${productionRestart7d} in 7d); ${classifierReplayText}; ${recoveryIntentText}; executor has 0 executed events`
    : "production restart ownership not proven in public boundary snapshot";

  if (executed.length) {
    return {
      state: "ok",
      label: "executed seen",
      meta: `${executed.length} executed evidence item(s), latest ${eventTimestamp(executed[0]) || "n/a"}`,
      executed,
      shadow,
      gated,
      latest,
      productionRestart30d,
      productionRestart7d,
      recoveryIntent30d,
      recoveryIntent7d,
      recoveryFalsePositive30d,
      recoveryFalsePositive7d,
      classifierReplay30d,
      classifierReplay7d,
      classifierEligible30d,
      classifierCovered30d,
      classifierUncovered30d,
      classifierEligible7d,
      classifierCovered7d,
      classifierUncovered7d,
      hasClassifierReplay,
      ownerText,
    };
  }

  if (shadow.length) {
    return {
      state: "ok",
      label: productionRestart30d ? "runtime-owned" : "shadow gated",
      meta: `${ownerText} / latest executor sample ${eventTimestamp(shadow[0]) || "n/a"}`,
      executed,
      shadow,
      gated,
      latest,
      productionRestart30d,
      productionRestart7d,
      recoveryIntent30d,
      recoveryIntent7d,
      recoveryFalsePositive30d,
      recoveryFalsePositive7d,
      classifierReplay30d,
      classifierReplay7d,
      classifierEligible30d,
      classifierCovered30d,
      classifierUncovered30d,
      classifierEligible7d,
      classifierCovered7d,
      classifierUncovered7d,
      hasClassifierReplay,
      ownerText,
    };
  }

  return {
    state: "unknown",
    label: "unknown",
    meta: "no recovery orchestrator evidence in public snapshot",
    executed,
    shadow,
    gated,
    latest,
    productionRestart30d,
    productionRestart7d,
    recoveryIntent30d,
    recoveryIntent7d,
    recoveryFalsePositive30d,
    recoveryFalsePositive7d,
    classifierReplay30d,
    classifierReplay7d,
    classifierEligible30d,
    classifierCovered30d,
    classifierUncovered30d,
    classifierEligible7d,
    classifierCovered7d,
    classifierUncovered7d,
    hasClassifierReplay,
    ownerText,
  };
}

function sparklinePath(points) {
  if (!points || points.length < 2) return "";
  const values = points.map((point) => Number(point.value)).filter(Number.isFinite);
  if (values.length < 2) return "";
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const usable = points.filter((point) => Number.isFinite(Number(point.value)));
  return usable
    .map((point, index) => {
      const x = (index / Math.max(1, usable.length - 1)) * 120;
      const y = 34 - ((Number(point.value) - min) / span) * 30;
      return `${index === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
}

function renderTrends(prom) {
  const trends = prom?.trends || [];
  const target = byId("trendGrid");
  clearNode(target);
  byId("trendCount").textContent = `${trends.length} sanitized series`;

  if (!trends.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No trend snapshot published";
    target.appendChild(empty);
    return;
  }

  for (const trend of trends) {
    const state = trend.status === "ok" && trend.points?.length ? "ok" : "warn";
    const card = document.createElement("article");
    card.className = `trend-card ${cssState(state)}`;

    const label = document.createElement("span");
    label.textContent = trend.label || trend.id;
    const value = document.createElement("strong");
    value.textContent = formatScalar(trend.latest, trend.unit);
    const meta = document.createElement("small");
    meta.textContent = `min ${formatScalar(trend.min, trend.unit)} / max ${formatScalar(trend.max, trend.unit)}`;

    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "sparkline");
    svg.setAttribute("viewBox", "0 0 120 36");
    svg.setAttribute("preserveAspectRatio", "none");
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", sparklinePath(trend.points || []));
    svg.appendChild(path);

    card.append(label, value, meta, svg);
    target.appendChild(card);
  }
}

function makeEvidenceCard(title, state, label, meta) {
  const card = document.createElement("article");
  card.className = `evidence-card ${cssState(state)}`;
  const name = document.createElement("span");
  name.textContent = title;
  const value = document.createElement("strong");
  value.textContent = label;
  const detail = document.createElement("small");
  detail.textContent = meta;
  card.append(name, value, detail);
  return card;
}

function renderEvidence(loki, freshness, autonomy) {
  const priority7d = sectionById(loki, "priority_7d");
  const priorityCount = priority7d?.event_count ?? 0;
  const watchdog = sectionEvents(loki, "youtube_watchdog")[0] || null;
  const monitoringState = watchdog?.healthy === true || watchdog?.status === "ok" ? "ok" : "warn";
  const gateCount = autonomy.gated.length;
  const target = byId("evidenceGrid");
  clearNode(target);
  byId("evidenceCount").textContent = `${priorityCount} priority / ${gateCount} gated / ${autonomy.productionRestart30d || 0} production restarts`;
  const ownershipState = autonomy.productionRestart30d
    ? (autonomy.hasClassifierReplay ? "ok" : "warn")
    : "warn";

  target.appendChild(makeEvidenceCard(
    "Mirror freshness",
    freshness.state,
    freshness.label,
    freshness.meta,
  ));

  target.appendChild(makeEvidenceCard(
    "Monitoring evidence",
    monitoringState,
    monitoringState === "ok" ? "active" : "unclear",
    watchdog
      ? `${watchdog.evidence_reason || watchdog.judgment_reason || "latest watchdog event"} / ${eventTimestamp(watchdog) || "n/a"}`
      : "no youtube_watchdog event in public snapshot",
  ));

  target.appendChild(makeEvidenceCard(
    "Priority replay",
    "ok",
    priorityCount > 0 ? `${priorityCount} event(s)` : "clear",
    priorityCount > 0
      ? `latest ${eventTimestamp(priority7d.events?.[0] || {}) || "n/a"}`
      : "no priority incident in the 7d public Loki retention",
  ));

  target.appendChild(makeEvidenceCard(
    "Recovery ownership",
    ownershipState,
    autonomy.classifierEligible30d ? `${autonomy.classifierCovered30d}/${autonomy.classifierEligible30d} replay` : autonomy.productionRestart30d ? "runtime-owned" : "unproven",
    autonomy.productionRestart30d
      ? `${autonomy.ownerText}; ${boundaryInterpretation(loki)}`
      : autonomy.ownerText,
  ));

  target.appendChild(makeEvidenceCard(
    "Recovery execution",
    autonomy.executed.length || autonomy.productionRestart30d || autonomy.shadow.length ? "ok" : "unknown",
    autonomy.executed.length
      ? "executed"
      : autonomy.productionRestart30d
        ? "runtime-owned"
        : autonomy.shadow.length ? "shadow gated" : "unknown",
    autonomy.meta,
  ));

  target.appendChild(makeEvidenceCard(
    "Gate behavior",
    gateCount ? "ok" : "warn",
    gateCount ? "blocking seen" : "no gate evidence",
    gateCount ? `${gateCount} gated plan event(s) in 30d snapshot` : "no sanitized gate event found",
  ));
}

function eventFields(event) {
  return eventKeys.filter((key) => Object.prototype.hasOwnProperty.call(event, key));
}

function collectEvents(loki) {
  const events = [];
  for (const section of loki?.sections || []) {
    if (["recovery_orchestrator_30d", "recovery_action_plan_30d"].includes(section.id)) continue;
    for (const event of section.events || []) {
      events.push({ section: section.label || section.id, ...event });
    }
  }
  const sorted = events.sort((a, b) => String(b.ts_ns || "").localeCompare(String(a.ts_ns || "")));
  const informative = sorted.filter((event) => eventFields(event).length || event.message);
  return (informative.length ? informative : sorted).slice(0, 16);
}

function chipValue(value) {
  if (value === null || value === undefined || value === "") return "n/a";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(2);
  return String(value);
}

function renderEvents(loki) {
  const events = collectEvents(loki);
  const target = byId("eventList");
  clearNode(target);
  byId("eventCount").textContent = `${loki?.summary?.total_events ?? 0} events in snapshot`;

  if (!events.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No recent events";
    target.appendChild(empty);
    return;
  }

  for (const event of events) {
    const row = document.createElement("article");
    row.className = "event-row";

    const meta = document.createElement("div");
    meta.className = "event-meta";
    const time = document.createElement("strong");
    time.textContent = eventTimestamp(event) || "n/a";
    const source = document.createElement("span");
    source.textContent = `${event.section || "event"} / ${event.source || "stream_v3"}`;
    meta.append(time, source);

    const fields = document.createElement("div");
    fields.className = "event-fields";
    const present = eventFields(event).slice(0, 8);
    for (const key of present) {
      const chip = document.createElement("span");
      chip.className = "chip event-chip";
      chip.textContent = `${key}: ${chipValue(event[key])}`;
      fields.appendChild(chip);
    }
    if (!present.length && event.message) {
      const message = document.createElement("span");
      message.className = "chip event-chip";
      message.textContent = event.message;
      fields.appendChild(message);
    }

    row.append(meta, fields);
    target.appendChild(row);
  }
}

async function loadJson(url) {
  const res = await fetch(`${url}?t=${Date.now()}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`);
  return res.json();
}

async function refresh() {
  byId("snapshotLine").textContent = "snapshot loading";
  const [prom, loki, reliability] = await Promise.all([
    loadJson("./stream-v3-prometheus.json"),
    loadJson("./stream-v3-loki.json"),
    loadJson("./reliability-indicators.json").catch(() => null),
  ]);
  const map = metricMap(prom);
  const promState = prom?.summary?.severity || "unknown";
  const lokiState = loki?.summary?.severity || "unknown";
  const freshness = snapshotFreshness(prom, loki);
  const autonomy = autonomySummary(loki);
  const mapRuntime = mapRuntimeSummary(map);
  const overall = worstState([promState, lokiState, freshness.state]);
  const youtubeMetrics = ["current_fail_1h", "youtube_issue", "ingest_issue", "same_url_issue"]
    .map((id) => map.get(id))
    .filter(Boolean);
  const youtubeState = worstState(youtubeMetrics.map((item) => item.state || "unknown"));

  setStateBlock(
    "overall",
    overall,
    stateText(overall),
    `${prom?.summary?.ok ?? 0}/${prom?.summary?.total ?? 0} metrics OK / Loki ${stateText(lokiState)} / snapshot ${freshness.label}`,
  );
  setStateBlock("snapshot", freshness.state, freshness.label, freshness.meta);
  setStateBlock(
    "youtube",
    youtubeState,
    youtubeState === "ok" ? "LIVE" : stateText(youtubeState),
    youtubeMetrics.map((item) => `${item.label}: ${formatValue(item)}`).join(" / "),
  );
  setStateBlock("map", mapRuntime.state, mapRuntime.label, mapRuntime.meta);
  setStateBlock("autonomy", autonomy.state, autonomy.label, autonomy.meta);

  renderChecks(map);
  renderReliability(reliability);
  renderGuards(map);
  renderTrends(prom);
  renderEvidence(loki, freshness, autonomy);
  renderEvents(loki);
  byId("metricCount").textContent = `${prom?.summary?.total ?? 0} public metrics`;
  byId("snapshotLine").textContent = `last snapshot: ${formatTime(freshness.oldest || prom?.generated_at)} / age ${freshness.label} / GCS + Cloudflare`;
}

byId("refresh").addEventListener("click", () => {
  refresh().catch((error) => {
    byId("snapshotLine").textContent = error.message || String(error);
  });
});

refresh().catch((error) => {
  byId("snapshotLine").textContent = error.message || String(error);
  setStateBlock("overall", "warn", "unknown", "snapshot load failed");
  setStateBlock("snapshot", "bad", "missing", "snapshot load failed");
  setStateBlock("youtube", "warn", "unknown", "snapshot load failed");
  setStateBlock("map", "warn", "unknown", "snapshot load failed");
  setStateBlock("autonomy", "warn", "unknown", "snapshot load failed");
  renderReliability(null);
});
