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
  {
    title: "Kubernetes container restarts (current Pod)",
    metric: "map_runtime_restarts",
    detail: "Kubernetes restartCount; resets when the Pod is replaced",
  },
  {
    title: "Fast Recovery dispatches (last 1h)",
    metric: "restarts_1h",
    detail: "controller restart-kind dispatches; may target an FFmpeg child",
  },
  {
    title: "FFmpeg incident clusters (last 1h)",
    metric: "ffmpeg_clusters_1h",
    detail: "restart attempts grouped within 10m; not the 7-day child total",
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

function appendGuardCard(target, { title, value, detail, state }) {
  const card = document.createElement("article");
  card.className = `guard ${cssState(state || "unknown")}`;

  const label = document.createElement("span");
  label.textContent = title;
  const strong = document.createElement("strong");
  strong.textContent = value;
  const small = document.createElement("small");
  small.textContent = detail;

  card.append(label, strong, small);
  target.appendChild(card);
}

function renderGuards(map, autonomy) {
  const target = byId("guardGrid");
  clearNode(target);

  for (const row of guardRows) {
    const item = map.get(row.metric);
    appendGuardCard(target, {
      title: row.title,
      value: formatValue(item),
      detail: row.detail,
      state: item?.state || "unknown",
    });
  }

  const activity = autonomy?.activity;
  appendGuardCard(target, {
    title: "Recovery activity",
    value: autonomy?.activityAvailable ? `${activity.total} / ${activity.days}d` : "n/a",
    detail: autonomy?.activityAvailable
      ? `stream/runtime ${activity.counts.restart_stream} · FFmpeg child ${activity.counts.restart_ffmpeg} · browser ${activity.counts.restart_browser} · JST`
      : "7-day action/outcome evidence unavailable",
    state: autonomy?.state || "unknown",
  });
}

function formatReliabilityValue(value, unit) {
  if (value === null || value === undefined) return "n/a";
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

function recoveryActivity(loki) {
  const activity = loki?.recovery_activity || {};
  const period = activity.period || {};
  const counts = activity.action_counts || {};
  const days = Number(period.days || 7);
  const total = Number(activity.total_executed || 0);
  const activeDays = Number(activity.active_day_count || 0);
  const averagePerDay = Number(activity.average_per_day || 0);
  return {
    status: activity.status || "unknown",
    days,
    total,
    activeDays,
    averagePerDay,
    counts: {
      restart_stream: Number(counts.restart_stream || 0),
      restart_ffmpeg: Number(counts.restart_ffmpeg || 0),
      restart_dj: Number(counts.restart_dj || 0),
      restart_browser: Number(counts.restart_browser || 0),
    },
    startDate: period.start_date || "",
    endDate: period.end_date || "",
    timezone: period.timezone || "Asia/Tokyo",
    latestActionAt: activity.latest_action_at_utc || "",
    sourceComplete: activity.source_complete === true,
    countBasis: activity.count_basis || "",
    scopeNote: activity.scope_note || "",
  };
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
  const activity = recoveryActivity(loki);
  const boundaryFresh = loki?.recovery_boundary?.fresh === true;
  const recoveryIntent7d = boundaryScalar(loki, "last_7d", "shadow_recovery_intent_action_count");
  const recoveryFalsePositive7d = boundaryReasonCount(loki, "last_7d", "false_positive_shadow");
  const hasRecoveryIntentSli = boundaryHasField(loki, "last_7d", "shadow_recovery_intent_action_count");
  const classifierReplay7d = boundaryClassifierReplay(loki, "last_7d");
  const classifierEligible7d = Number(classifierReplay7d.eligible_count || 0);
  const classifierCovered7d = Number(classifierReplay7d.covered_count || 0);
  const classifierUncovered7d = Number(classifierReplay7d.uncovered_count || 0);
  const hasClassifierReplay = boundaryFresh
    && boundaryHasField(loki, "last_7d", "current_classifier_replay");
  const activityAvailable = ["ok", "partial"].includes(activity.status);
  const activityState = activity.status === "ok" ? "ok" : activity.status === "partial" ? "warn" : "unknown";
  const activityMeta = activityAvailable
    ? `${activity.activeDays}/${activity.days} active days · avg ${activity.averagePerDay.toFixed(1)}/day · last ${activity.days} JST days`
    : "current production recovery activity unavailable";
  return {
    state: activityState,
    label: activityAvailable ? `${activity.total} recoveries` : "unknown",
    meta: activityMeta,
    executed,
    shadow,
    gated,
    latest,
    activity,
    activityAvailable,
    boundaryFresh,
    recoveryIntent7d,
    recoveryFalsePositive7d,
    classifierReplay7d,
    classifierEligible7d,
    classifierCovered7d,
    classifierUncovered7d,
    hasClassifierReplay,
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

function makeRecoveryActivityCard(autonomy) {
  const activity = autonomy.activity;
  const card = document.createElement("article");
  card.className = `evidence-card recovery-activity-card ${cssState(autonomy.state)}`;

  const head = document.createElement("div");
  head.className = "recovery-card-head";
  const name = document.createElement("span");
  name.textContent = "Recovery activity";
  const coverage = document.createElement("span");
  coverage.className = `recovery-coverage ${activity.sourceComplete ? "complete" : "partial"}`;
  coverage.textContent = activity.sourceComplete ? "complete log" : "partial log";
  head.append(name, coverage);

  const summary = document.createElement("div");
  summary.className = "recovery-summary";

  const total = document.createElement("div");
  total.className = "recovery-total";
  const totalValue = document.createElement("strong");
  totalValue.textContent = autonomy.activityAvailable ? String(activity.total) : "—";
  const totalLabel = document.createElement("span");
  totalLabel.textContent = "recoveries";
  const totalWindow = document.createElement("small");
  totalWindow.textContent = `Last ${activity.days} JST days`;
  total.append(totalValue, totalLabel, totalWindow);

  const stats = document.createElement("dl");
  stats.className = "recovery-stats";
  const statItems = [
    ["Active days", autonomy.activityAvailable ? `${activity.activeDays}/${activity.days}` : "—"],
    ["Daily average", autonomy.activityAvailable ? activity.averagePerDay.toFixed(1) : "—"],
  ];
  for (const [label, value] of statItems) {
    const stat = document.createElement("div");
    const term = document.createElement("dt");
    term.textContent = label;
    const detail = document.createElement("dd");
    detail.textContent = value;
    stat.append(term, detail);
    stats.appendChild(stat);
  }
  summary.append(total, stats);

  const actions = document.createElement("div");
  actions.className = "recovery-actions";
  const items = [
    ["Stream / runtime", activity.counts.restart_stream],
    ["FFmpeg child", activity.counts.restart_ffmpeg],
    ["Auto DJ", activity.counts.restart_dj],
    ["Browser helper", activity.counts.restart_browser],
  ];
  for (const [label, count] of items) {
    const action = document.createElement("div");
    action.className = "recovery-action";
    const actionHead = document.createElement("div");
    actionHead.className = "recovery-action-head";
    const actionLabel = document.createElement("span");
    actionLabel.textContent = label;
    const actionValue = document.createElement("b");
    actionValue.textContent = String(count);
    actionHead.append(actionLabel, actionValue);
    const track = document.createElement("div");
    track.className = "recovery-action-track";
    const fill = document.createElement("i");
    fill.style.setProperty(
      "--recovery-share",
      `${activity.total > 0 ? Math.min(100, (count / activity.total) * 100) : 0}%`,
    );
    track.appendChild(fill);
    action.append(actionHead, track);
    actions.appendChild(action);
  }

  const detail = document.createElement("small");
  detail.className = "recovery-footnote";
  detail.textContent = autonomy.activityAvailable
    ? `Production action/outcome evidence · ${activity.scopeNote || "failure domains are counted separately"} · JST ${activity.startDate}–${activity.endDate}`
    : autonomy.meta;
  card.append(head, summary, actions, detail);
  return card;
}

function makeRecoveryOwnershipCard(autonomy) {
  const state = autonomy.boundaryFresh && autonomy.hasClassifierReplay ? "ok" : "warn";
  const card = document.createElement("article");
  card.className = `evidence-card recovery-ownership-card ${cssState(state)}`;

  const name = document.createElement("span");
  name.textContent = "Recovery ownership";
  const value = document.createElement("strong");
  value.textContent = autonomy.boundaryFresh ? "Runtime-owned" : "Boundary stale";

  const facts = document.createElement("dl");
  facts.className = "recovery-boundary-facts";
  const items = [
    ["Classifier replay", autonomy.hasClassifierReplay ? `${autonomy.classifierCovered7d}/${autonomy.classifierEligible7d}` : "n/a"],
    ["Uncovered", autonomy.hasClassifierReplay ? autonomy.classifierUncovered7d : "n/a"],
    ["Shadow intent", autonomy.boundaryFresh ? autonomy.recoveryIntent7d : "n/a"],
    ["False positive", autonomy.boundaryFresh ? autonomy.recoveryFalsePositive7d : "n/a"],
  ];
  for (const [label, factValue] of items) {
    const term = document.createElement("dt");
    term.textContent = label;
    const detail = document.createElement("dd");
    detail.textContent = String(factValue);
    facts.append(term, detail);
  }

  const note = document.createElement("small");
  note.textContent = `Shadow observes only · orchestrator executed ${autonomy.executed.length} · watchdog, fast-recovery and stream-engine execute production actions`;
  card.append(name, value, facts, note);
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
  byId("evidenceCount").textContent = autonomy.activityAvailable
    ? `${autonomy.activity.total} recoveries / ${autonomy.activity.activeDays} active days / ${autonomy.activity.days}d JST`
    : `${priorityCount} priority / ${gateCount} gated`;

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

  target.appendChild(makeRecoveryActivityCard(autonomy));

  target.appendChild(makeRecoveryOwnershipCard(autonomy));

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
  renderGuards(map, autonomy);
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
