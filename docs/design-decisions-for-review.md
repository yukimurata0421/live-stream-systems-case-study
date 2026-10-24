# Design Decisions For Review

This is the decision ledger for `stream_v3`, written for reviewers who want to
understand the engineering judgment behind the system without reading every
runtime document. Each row states a decision, why it was made, what was
rejected, and what it costs. Detailed contracts and evidence are linked; the
[implementation review map](implementation-review-map.md) is the shortest path
from a claim to code and tests.

One pattern runs through the whole project: delivery, observation, source data,
recovery, and public presentation are separate failure domains. A dashboard
warning is not automatically a delivery failure, and a delivery symptom is not
automatically permission to restart something or change YouTube state.

## How To Read This Ledger

- Rows describe **current production** unless marked otherwise.
  **Design only** means the source exists in this repository but holds no
  production authority. **Deferred** means a considered option was postponed,
  with the reason stated. **Disabled** means a capability exists and is
  intentionally switched off.
- Many rows began as incidents in the earlier `/stream` and `stream_v2`
  systems. They are kept as constraints that still shape v3, not as history.
  The "Why" column names the origin when it matters.
- State that is expected to change soon lives only in
  [current status](current-status.md); this ledger records decisions.

## Start Here

The decisions that most shape the current system:

| Decision | Where |
| --- | --- |
| One home upload to an OCI relay that fans out to YouTube and Twitch, with a direct YouTube fallback that keeps working when monitoring is down. | [Media route](#media-route-and-delivery-recovery) |
| An unfinished recovery design was replaced after 104 qualification runs instead of being patched further. | [Media route](#media-route-and-delivery-recovery), [decision record](v3/oci-route-simplification-decision.md) |
| A WAN gap reconnects only the FFmpeg output, in the same process; riding through on the old connection is deferred until a delivery judge can be calibrated. | [Media route](#media-route-and-delivery-recovery), [runtime contract](v3/ffmpeg-fifo-recovery.md) |
| The public YouTube URL is a production invariant; replacement broadcasts are disabled. | [YouTube lifecycle](#youtube-lifecycle-and-identity) |
| No single signal decides health, and no observation layer claims viewer playback. | [Observation](#observation-sli-and-notification) |
| A stricter central restart authority exists as a design and runs only in `dry_run`. | [Recovery authority design](#recovery-authority-design) |

## Architecture And Ownership

| Area | Decision | Why | Rejected alternative | Trade-off |
| --- | --- | --- | --- | --- |
| Runtime boundary | Use k3s for `stream_v3` delivery. | Browser rendering, audio, FFmpeg, watchdogs, and recovery competed inside earlier single-host ownership. | Systemd-only ownership for the whole stack. | More operational complexity, but explicit container ownership, resource limits, state mounts, and dry-run validation. |
| Runtime ownership | Keep `stream-v3-runtime` as a 4-container Pod: `stream-engine`, `precipitation-fetcher`, `auto-dj`, `fast-recovery-loop`. | Rendering, weather, music, and local recovery fail differently but share delivery state. | One process, or DJ/recovery back on host services. | More wiring, but restart counts and logs show which component moved. |
| Host split | Delivery on Dell, monitoring on the HP ProDesk (`arena-server`), public publication on a Raspberry Pi. Observation holds facts, not FFmpeg control. | Long-window SLI, dashboards, and notifications had to survive delivery-host pressure; the observer should not issue shell actions. | Delivery, monitoring, and recovery on one host. | The ProDesk also receives ADS-B, so its outage affects both source data and monitoring; the media route's self-healing does not depend on it. |
| Single publisher guard | Prevent competing publishers for one ingest target. The fallback sends to the separate YouTube **backup** ingest, only from relay-health loss until 300 s of recovered health. | Legacy `/stream` multi-start produced competing publishers on the primary ingest URL. | Let processes race and rely on YouTube to choose. | Locks, replica limits, and restart attribution add ceremony; the deliberate fallback upload is bounded by the route controller. |
| Ingest secret contract | Validate stream-key and output configuration before publishing; mask secrets in logs. | Placeholder or manual env mistakes could fail startup or expose the ingest URL. | Trust manual exports and log full destinations. | More startup checks, but logs and public evidence stay safe to share. |
| Public release scope | Present the repository as a case study, not a reusable streaming product. | The value is the reliability design, evidence, and safety model, not an installer. | Package a plug-and-play streaming stack. | Reviewers see real judgment without assuming their environment can run it unchanged. |

## Media Route And Delivery Recovery

| Area | Decision | Why | Rejected alternative | Trade-off |
| --- | --- | --- | --- | --- |
| Self-healing relay route | One normal home upload to an OCI relay that forwards to YouTube and Twitch. A Dell controller starts a direct YouTube fallback when relay health is lost or unobservable and stops it after 300 s of continuous health. | The home uplink constrained permanent separate feeds, and YouTube stays the first priority. | Separate permanent feeds from home, or a permanent backup upload. | Fixed thresholds instead of adaptive policy; the fallback shares the Dell host, power, and uplink. See the [route record](v3/self-healing-oci-fanout.md). |
| Route simplification (2026-10-20) | Replace the unfinished signed-lease route and its central authorization with a restart-always relay, a forward keeper, and one fallback controller. | After 104 qualification runs the complete path was still unfinished, and its one-shot backup did not reconnect after a short network drop while the main sender recovered in five seconds. | Keep patching the lease, authority, and qualification machinery. | Loses lease fencing and central authorization for this route; gains a path that keeps working when monitoring or authorization is down. See the [decision record](v3/oci-route-simplification-decision.md). |
| Relay host | Run the relay on an OCI VM. | OKE capacity in Tokyo stayed unavailable for about two weeks; continuous-video transfer cost was also a constraint. | Keep waiting for managed Kubernetes capacity. | The relay runs as VM services rather than Pods; no independent capacity benchmark or bill comparison is claimed. |
| Twitch session renewal | The OCI keeper renews only the Twitch forward (daily or at 46 h session age) and preserves the YouTube forward. | Twitch sessions need periodic renewal; YouTube continuity must not depend on it. | Restart the whole relay, or keep the [earlier separate renewal owner](v3/twitch-session-renewal.md). | YouTube continuity across a keeper-driven renewal is not yet separately verified ([current status](current-status.md#not-yet-observed)). |
| Output connection recovery (2026-10-24) | Reconnect only the RTMPS output inside the same FFmpeg process with the FIFO muxer; keep process replacement for FFmpeg exit. | A WAN gap is a network event and does not need a new encoder or capture inputs. Reconnection has one explicit timeout, and a wrong value shows up as an extra reconnect. | Restart FFmpeg for every output failure. | Measured FIFO reconnects were not consistently faster than restarts; the value is keeping the encoder. See the [runtime contract](v3/ffmpeg-fifo-recovery.md) and [measurements](v3/ffmpeg-fifo-measurements.md). |
| Ride-through on the existing connection | **Deferred.** Keep the TCP connection open across a WAN gap instead of reconnecting. | Judging that a surviving connection delivers correctly needs structured freshness and jitter evidence and calibrated thresholds, which do not exist yet. An uncalibrated judge fails silently. | Adopt ride-through with guessed thresholds. | An output I/O timeout costs a reconnect; its value is not a fixed deadline from the start of the WAN gap. Transport telemetry collects evidence for revisiting it. |
| Restart actors | Fast recovery inside the Pod may restart only its own FFmpeg child. The `arena-server` monitor may request a guarded runtime restart (budget, cooldown, 180 s convergence wait). | RTMPS facts are closest to the process; broader restarts need budget and evidence from outside the Pod. | Let the observer act on FFmpeg directly, or give every loop unscoped restart authority. | Two scoped actors remain. The [CRA design](#recovery-authority-design) that would unify action selection is not enabled. |
| Recovery cadence | Keep evidence cadence short without turning it into repeated effect cadence. | Same-URL recovery is time-sensitive, but connectivity loss cannot be fixed by rapid child relaunches. | One global exponential backoff, or restart on every short sample. | Probes stay responsive while network-down waits, budgets, and cooldown control action. |
| Bounded FFmpeg stop | Stop stuck FFmpeg inside the engine before the supervisor force-kills the parent. | TCP stalls can make graceful shutdown exceed service or Pod stop timeouts. | Increase stop timeouts and wait longer. | Faster recovery and better evidence, at the cost of a less graceful muxer close. |
| Host freeze recovery | Add a host watchdog policy for the single-node delivery host. | In-Pod recovery cannot recover a frozen OS, PID1 stall, or kernel lockup. | Assume k3s or fast recovery covers every failure. | `softdog` and systemd watchdog help; a hardware watchdog still depends on BIOS support. |
| DR model | Document single-node DR and rollback separately from live recovery. | Node rebuild, reboot, rollback, and FFmpeg restart have different risk and RTO. | One generic recovery playbook. | More runbooks, but scoped response. Single-node k3s is not an HA claim. |
| Relay backup | Back up the relay as encrypted, files-only bundles; restoring files is not restoring the service. | A replacement relay needs matching executable, source, units, and configuration verified before anything starts. | Treat a restored file set as a recovered relay. | A new-VM bootstrap from the bundle is not yet exercised. See [OCI backup and restore](v3/oci-backup-and-restore.md). |
| Legacy rollback | Keep older repo/state/env rollback paths explicit, not automatic. | v1/v2 cutovers changed units, CLI symlinks, state roots, and env ownership. | Delete old roots immediately or auto-fall back on faults. | More archived surface, but rollback does not depend on memory. |

## Recovery Authority Design

**Design only.** These rows describe the Central Restart Authority (CRA) and its
local reflex path. The source is in [`recovery-control/`](../recovery-control/README.md);
production authority is disabled, and the current media route does not use it.
The production restart actors are listed [above](#media-route-and-delivery-recovery).

| Area | Decision | Why | Rejected alternative | Trade-off |
| --- | --- | --- | --- | --- |
| Authority split | Monitoring publishes facts only; `cra-01` owns policy, budget, cooldown, and authorization; Dell admits at most one exact, fenced FFmpeg-child effect. | Independent decision paths had formed duplicate restart scopes. | Let the observer or every local loop authorize FFmpeg independently. | More typed transport and reconciliation for smaller authority domains. See [scoped recovery authority](v3/scoped-recovery-authority.md). |
| Transaction boundaries | Apply transaction concepts to intent, outbox, command, admission, effect fence, and reconciliation. | Duplicate scopes and ambiguous responses need more than request-level deduplication. | Treat retries as independent, or describe the path as distributed ACID. | Local atomic boundaries, not a global transaction; more protocol and persistence work. |
| Unknown outcome | Never treat `OUTCOME_UNKNOWN` as permission for a new request identifier. | A response may be lost after the physical effect began. | Retry immediately for availability. | Recovery can pause for reconciliation, but uncertainty cannot become a second restart. |
| Restart scope | Admit only the exact FFmpeg child, never a broader restart. | Child recovery is narrower than restarting the browser, PulseAudio, AutoDJ, a Pod, or a host. | Escalate an absent child to a broader restart. | Exact identity and reconciliation add ceremony but prevent scope expansion. |
| Local reflex (2026-10-22, `dry_run`) | Decide each FFmpeg-child restart on Dell under a policy signed by `cra-01`; budget limits come from `cra-01`, Dell counts against them, and the audit is verified centrally afterward. | Per-action central approval also needed a separate LAN-loss decider, so two deciders handed authority back and forth through leases, epochs, and journal import. | Per-action approval on `cra-01` with a local fallback mode. | One decider in every network state, but no central veto during LAN loss. It records would-restart decisions and sends no restart requests. See the [design record](../recovery-control/docs/local-reflex-authority.md). |

## YouTube Lifecycle And Identity

| Area | Decision | Why | Rejected alternative | Trade-off |
| --- | --- | --- | --- | --- |
| Same URL | Treat same-URL continuity as a production invariant, not part of availability. | A replacement broadcast can keep "a stream" online while losing viewers, bookmarks, embeds, and external links. | Replace broadcasts whenever YouTube evidence is ambiguous. | Recovery must preserve identity. See [same-URL evidence](same-url-continuity-evidence.md). |
| Broadcast replacement | **Disabled** since 2026-10-20. A broadcast that really ends is recreated by hand. | Same-URL continuity outranks keeping "a stream" online, and with a persistent scheduled broadcast (`enableAutoStop=false`) elapsed time alone does not prove the URL is unrecoverable. | Replace after a fixed window, even behind gates. | A real end needs operator action; automated replacement cannot break the URL. |
| YouTube identity | Resolve and monitor the current video ID through resolver and watchdog state. | Public live state, OAuth/Data API state, and cached URLs must be correlated. | Trust one source. | More state and cache handling, but fewer false conclusions. |
| Mutation gate | Require OAuth channel match, evidence ledger state, and an action gate before any destructive YouTube action. | Public page, Data API, OAuth, and cached state can disagree during outages. | Let watchdog code transition or bind broadcasts directly. | More machinery, but wrong-channel or stale-evidence mutations are blocked. See [YouTube lifecycle safety](v3/youtube-lifecycle-safety.md). |
| OAuth control plane | Treat OAuth token health as control-plane evidence, not delivery health. | `invalid_grant` can degrade mutation authority while ingest and public live stay healthy. | Restart delivery because an OAuth probe fails. | Credentials are repaired without confusing the control plane with viewer impact. |
| Public live proof | Separate public live availability from local ingest. | A connected sender does not prove a live watch page. | Treat ingest connected as public live. | Public probes are noisy, so negative evidence alerts rather than acts. |
| Resolver fast mode | Use fast resolver cadence only under unhealthy or confirmed conditions; configured IDs are boot seeds only. | v2 quota incidents showed high-cost search and stale IDs can amplify outages. | Search frequently or fall back to configured IDs. | Fast when needed without burning quota or reviving stale IDs. |
| Quota budget | Spend a measured quota budget on faster unhealthy-state resolution. | Routine checks used about 1.5k-1.8k units/day against a 10k/day quota. | Slow every check, or call search every 5 s. | Expensive calls stay confirmed and gated. |
| Quota exhaustion | Treat quota exhaustion as degraded evidence and prefer transport, public, and local evidence while it lasts. | 403 responses make Data API/OAuth state less authoritative, not the stream failed. | Treat API errors as stream failure or as normal lifecycle truth. | Source-weighted decisions instead of API-first ones. |
| Quota day accounting | Show Pacific-Time open-day actual units separately from projections and closed days. | Quota resets on Pacific Time; projection must not look like usage. | Show projected or last closed day as today. | More metrics, but real burn is visible. |

## Observation, SLI And Notification

| Area | Decision | Why | Rejected alternative | Trade-off |
| --- | --- | --- | --- | --- |
| Health model | Judge health from multiple signals, not one dashboard stat. | Delivery needs YouTube live, ingest, Pod, capture, audio, upload, watchdog, and freshness evidence. | Let one panel decide health. | Slightly slower diagnosis, fewer restarts from stale panels. |
| Video observation layers | Observe the relay return feed, fallback sender, native forward counters, provider APIs, and Twitch thumbnails as separate layers. None holds recovery authority or claims viewer playback. | Each layer sees a different point at a different cadence; a disagreement is something to diagnose. | Pick one layer as end-to-end truth, or wire clock layers to restarts. | No end-to-end success claim exists. See [video observation layers](observability.md#video-observation-layers). |
| Public player acquisition | **Deferred** pending applicable terms or permission. | Automated acquisition of the public player needs applicable terms or permission. | Scrape the public player. | Public-player continuity and platform delay remain unobserved. |
| Dashboard interpretation | Classify anomalies as current incident, historical event, or observability noise. | Long-window or stale series can show `FAIL` after recovery. | Treat every visible failure as current impact. | Raw metrics must be checked, but false positives do not burn error budget. |
| Active notification gate | Notify only from fresh current-fail signals, not historical windows. | v2 Discord loops made recovered history look active. | Alert on every 1h/24h warning count. | History stays in routine checks; alerts describe current impact. |
| Notification routing | In the OCI-aware Twitch notifier, relay, input, or Twitch-output interruptions become Critical on Slack after three minutes; observation and data-quality warnings go to Discord. | Operators need to separate "delivery is broken" from "evidence is degraded". | One channel for everything. | Real Critical delivery through the Twitch notifier is not yet observed. |
| Recovery naming | Separate restart actions, unconfirmed recovery, and confirmed auto-recovery in notifications. | A restart is not a recovery until fresh evidence shows it. | Report every restart as recovered. | More message types. See [notification and auto-recovery](v3/notification-and-auto-recovery.md). |
| Report-only promotion | Keep weak probes report-only until repeated low-false-positive correlation exists. | Map movement, public probes, TLS strings, and upstream reports are useful but noisy. | Wire every probe to restart or alert control. | Some real weak signals wait for review; recovery stays stable. |
| Visual report-only boundary | Map-runtime and public-viewer probes are read-only unless viewer impact repeats and correlates. | Map, browser, weather, playback, and capture failures have different owners. | Restart on the first probe failure. | Two repeated black/frozen samples can page; recovery still needs correlated evidence. |
| Network observer boundary | Route, DNS, IPv4/IPv6, and TCP-connect cause layers stay observer-only until correlation is proven. | Cause-layer probes explain recurrence but are noisy. | Promote cause labels to recovery triggers. | Better evidence without wider blast radius. See the [TCP stall case study](v3/tcp-stall-case-study.md). |
| Transport evidence | Use fresh sender-side TCP samples for the Dell-to-relay segment; relay forward counters cover each provider. | The observability host cannot inspect in-Pod sockets, and since 2026-10-20 the sender's TCP peer is the relay, not YouTube. | Mark network failed when the observer cannot see the socket, or read sender TCP as provider health. | More exporter logic, but each segment has its own evidence. |
| Upload SLI | Use windowed TCP samples for upload latest, p95, max, age, and over-budget seconds. | Older summary fields misrepresented `Upload p95 1h`. | Reuse 24h health fields. | More metrics, but tuning matches observed send behavior. |
| Upload source time | Bind TCP counters to producer timestamp, identity, generation, PID, and sequence. | Consumer integer-second timing distorted intervals and could make missing provenance look real. | Divide by consumer wall-clock time or backfill old logs. | New samples are recomputable and fail closed; older logs stay uncorrected. |
| SLI windows | Separate rolling, cumulative, and regime-bounded SLI. | Old unstable periods made cumulative ratios look like current quality. | One lifetime percentage. | More labels, but current regime and history stop conflicting. |
| Restart aggregation | Report FFmpeg restarts as attempts, retry episodes, incident clusters, root cause, and max duration. | Raw counts overstate independent incidents. | One restart = one incident. | More aggregation, but retry density and impact are visible. |
| SLO policy and priority | Track availability, same URL, input quality, upload ceiling, visual and audio correctness separately; prioritize availability and same URL, then input quality, then visual/audio, then upload efficiency. | Upload efficiency is not the product outcome; low upload is unacceptable if it causes YouTube input warnings. | Optimize bandwidth first, or treat all warnings as equal. | When budget burns, the operator states what changed and what did not. See [SLI methodology](sli-methodology.md). |
| Subsystem signals | Keep ADS-B freshness, API quota, notification state, visual, audio, upload, and availability distinct. | A live stream can still have stale ADS-B data, muted audio, or degraded evidence. | One availability percentage. | More numbers, each with an owner. |
| ADS-B availability and coverage | Report source availability (share of events active or quiet) separately from observation coverage (share of the window with a current event). | A quiet sky and a missing observer are different facts. | One freshness percentage. | Neither measures every ADS-B message or viewer reception. See [ADS-B coverage](v3/adsb-source-coverage.md). |
| Metrics namespace and isolation | Use `stream_v3_*` metrics with `job="stream_v3_observability_monitor"`; scope production and shadow queries by job/source. | v2-compatible names and broad `max()` queries let a shadow source turn production panels red. | Shared names relying on labels or titles. | Explicit queries, but red panels point to the right owner. |
| Exporter shape validation | Test exporter mappings against the actual health-summary JSON shape. | A panel read `window.pass` while the value lived under `window.observe.pass`. | Trust panels without raw checks. | Extra tests prevent believable false health. |
| Memory attribution and action | Classify anonymous memory, file cache, inactive file, one-shot peaks, and non-reclaimable estimates separately; never restart only because memory looks large. | File cache and one-shot peaks look large without being leaks. | Restart when Grafana memory looks high. | Operators read trend, limit ratio, events, and process breakdown. |
| Runtime memory guards | Measure the `stream-v3-runtime` Pod, not the observability host; add a process-level Xvfb RSS/shared-memory guard. | A prior OOM came from Xvfb shared-memory growth missed between container scrapes. | Host memory or container samples only. | More in-container inspection. See the [memory guard case study](v3/memory-guard-case-study.md). |

## Encoder And Transport

| Area | Decision | Why | Rejected alternative | Trade-off |
| --- | --- | --- | --- | --- |
| Encoder baseline | NVIDIA NVENC `h264_nvenc` CBR in production. | VBR/CQ reduced upload but caused YouTube low-bitrate warnings. | CPU `libx264`, or VBR/CQ for bandwidth. | More bandwidth for better input quality; CPU encode remains the fallback sender's path. |
| Encoder cadence | 5 fps with 192k audio; video moved from 3400k to 4000k on 2026-10-22 after the relay route drew YouTube `bitrateLow` warnings. | A 2026-05-31 4/5/10 fps comparison at constant bitrate favored 5 fps; after the bitrate change, warnings fell from 11 to 0 across the 24 hours before and after. | Stay at 4 fps, or promote 10 fps from a short trial. | Why the relayed stream drew warnings at an unchanged media rate is still unexplained. |
| Encoder lineage | Keep v2, v3 trials, and the current runtime named separately. | "Current FPS" changed meaning across migration and tuning. | One continuous encoder story. | Reviewers can follow 5→4 fps in v2, a 30 fps trial, and the measured 5 fps contract. |
| Encoder knobs | Do not promote NVENC quality knobs without env-sync evidence. | `p5`, multipass, lookahead, AQ, and B-frames were trial settings; the contract is p4 CBR. | Leave trial settings in the live ConfigMap. | Simpler contract; future changes need YouTube health and upload evidence. |
| Recovery profiles | Keep 2500k as a bounded emergency profile with expiry. | A July response left emergency values active for about 36 hours, making a config change look like uplink degradation. | Promote emergency values without a measurement decision. | Expiry and profile identity add checks, but upload history stays interpretable. |
| Upload ceiling | Treat 5.0 Mbps as a warning ceiling, not a tuning target. | Normal operation should stay below it without sacrificing YouTube health. | Force every profile into 4.0-4.5 Mbps. | Samples can approach the line; input quality is protected first. |
| RTMPS transport | RTMPS for the sender, now to the relay over a private tunnel. | Encrypted ingest, checked by contract tests. | Legacy RTMP as the normal path. | TLS adds failure modes and framing overhead: on 2026-10-21 an earlier 4.88 Mbps reading included about 34% TLS framing over about 3.6 Mbps of media. |
| TLS observability | Track SSL/TLS events separately from generic broken pipe or `tcp_stall`. | They need different triage. | Collapse all transport errors into one. | No restart by itself; sharper follow-up evidence. |
| A/V timestamps | Keep FFmpeg timestamp stabilization in the encoder contract. | x11grab and PulseAudio produced non-monotonic DTS/PTS. | Treat warnings as noise. | A less minimal command for steadier muxing. |

## Rendering, Audio And Content

| Area | Decision | Why | Rejected alternative | Trade-off |
| --- | --- | --- | --- | --- |
| ADS-B source boundary | Keep the Airspy/readsb chain distinct from delivery. | Airspy, `airspy_adsb`, both readsb instances, and the map endpoint fail differently from rendering or RTMPS. | Treat the map endpoint as a renderer symptom. | Explicit freshness checks; public manifests do not own the Airspy device. |
| Rendering source | Render from readsb/tar1090/custom map data, not from another stream. | The stream should produce its own video from source data. | Re-encode another stream. | More rendering ownership, clearer source-to-video evidence. |
| Browser capture | Capture the browser/Xvfb frame of the custom MapLibre output. | ADS-B, terrain, precipitation, and UI are composed in the browser; capture what exists. | Feed data straight into FFmpeg. | Heavier, but preserves real overlays, attribution, and defects. |
| Render warmup | Require current map-tile and ADS-B-sample evidence before declaring the browser warm. | A listening server or Chromium PID can still produce a blank frame. | Start FFmpeg right after launch. | Times out after 20 s and fails open, recording the failure. |
| Startup readiness | Gate FFmpeg on rendering/audio readiness instead of fixed sleeps. | `/stream` measurements showed overlay readiness, not a 20-25 s wait, was the constraint. | Large fixed sleeps. | Faster recovery; probes must stay meaningful. |
| Weather isolation | JMA precipitation is a degradable overlay, not delivery authority. | Weather can fail while aircraft, audio, and delivery stay healthy. | Restart when tiles are stale. | Weather disappears after a bounded last-known-good window without a restart loop. |
| Viewer UI boundary | Keep debug/status text out of the viewer frame. | Legacy incidents exposed stale fields and translation UI. | Show operational state on stream. | Operators use dashboards instead. |
| Actual range guard | Validate supplement data before merging it into the displayed range. | One bogus point distorted the range for hours. | Trust every persisted point. | Rare true outliers can be rejected. |
| Audio transport | Container PulseAudio with `--disable-shm=yes` and `--enable-memfd=no`. | shm/memfd caused container audio failures. | Default shared-memory transport. | Gives up an optimization for stability. |
| Audio health | Verify audio by Pulse route, sinks, sink inputs, monitor energy, and fault counters; stage recovery and suppress track-transition noise. | A running DJ does not prove audio reaches FFmpeg; song changes look like silence. | Process liveness as health, or restart on the first quiet sample. | Silent-stream detection waits for repeated evidence. |
| Audio quality | Keep 192k music audio as part of the product. | Small upload cost relative to video, better match to sources. | Minimize audio bitrate. | Slightly higher upload for a deliberate viewer feature. |
| Music evidence | Treat now-playing metadata, play history, and rotation classification as runtime evidence. | `Unknown title`, skipped playback, and missing audit history were real defects. | Treat music as an opaque background process. | More state files, investigable regressions. |
| Provider-specific playback | Reserve the evening bucket for the reviewed Floracore collection, require credit confirmation before activation, and normalize providers at playback time. | Provider permission, attribution, and loudness differ; modifying files would weaken provenance. | Mix providers, reuse one credit, or re-encode files. | Public tests prove the mechanism, not live activation or the private permission record. |
| Platform audio detection (2026-10-24) | Remove tracks flagged by a destination's automated audio detection from the shared AutoDJ rotation; keep source files and play history. Each destination carries its own credits. | Twitch's detection muted VOD segments for three credited-use NCS tracks; license permission does not prevent fingerprint detection. The same review found Twitch had carried no NCS credit from its addition until 2026-10-24. | Exclude tracks for Twitch only (impossible with one fanned-out feed without a second upload), or keep playing and rely on credits. | Exclusion is reactive and shrinks the rotation for every destination. Adding a destination now triggers a compliance re-review. See [platform audio detection](compliance-and-licensing-boundary.md#platform-audio-detection). |
| Compliance boundary | Document ADS-B radio, receiver privacy, and music attribution as constraints, not legal certification. | RF data, an aircraft map, and third-party music must not collapse into one assumption. | Leave it in private correspondence. | A re-review checklist, not legal advice. See [compliance](compliance-and-licensing-boundary.md). |

## Change Management And Publication

| Area | Decision | Why | Rejected alternative | Trade-off |
| --- | --- | --- | --- | --- |
| Production authority | Transfer production authority only through explicit cutover evidence. | During migration, v2 stayed production until units, CLI, state root, monitoring, and mutation paths were moved. | Treat a Running v3 Pod or green shadow check as ownership transfer. | More proof per cutover. See the [migration case study](v3/migration-cutover-case-study.md). |
| Migration smoke test | A 24-hour live smoke test gated the v2-to-v3 migration and cutover-authority changes. | v3 had to prove migrated ownership across one daily cycle. | A short launch test or a green Pod. | Later targeted changes (4000k on 2026-10-22, FIFO output on 2026-10-24) were checked by targeted exercises after rollout; their longer behavior is tracked in [current status](current-status.md). |
| Shadow mode and replay | Validate in non-mutating shadow mode before cutover; keep historical shadow gaps visible and publish current classifier replay separately. | Prove command mapping and guards before mutation without rewriting past logs. | Enable recovery directly, or backfill shadow history. | More steps; reviewers see both the gap and its remediation. |
| Cutover flags and maintenance | Require explicit production flags for live mutation; treat planned work as maintenance state. | Accidental mutation should be hard; intended work should not page as an outage. | Infer production from environment shape. | More configuration, clearer alerts and SLO reading. |
| Restart attribution | Record restart requests before manual rollout, staged restart, or remote recovery. | `restartedAt` shows a template change, not who asked or why. | Reconstruct causes later. | Pod replacement can be classified as manual, automatic, or fault. |
| Release provenance | Build immutable releases from verified source identity and test installed consumer contracts before switching a shared pointer. | A release once inherited an older checkout whose CLI rejected arguments used by report-only units. | Treat checkout location as identity. | Gates must cover the full consumer graph. |
| Public status | Publish a sanitized static snapshot from the Pi through GCS and Cloudflare; the Pi only pulls, and relay observation travels over the private tunnel. | Outside readers need freshness and guardrail evidence without home ingress, uplink read load, or access to Grafana, raw logs, or credentials. | Expose Grafana, or serve reads from home. | Less site source is public, but the boundary is clean. See [public status snapshot](v3/public-status-snapshot.md). |
| Evidence and retention | Keep runtime state and secrets out of Git; rotate raw JSONL and promote long-term evidence into docs. | Reviewers need reasoning without keys, tokens, or state; raw logs cannot grow forever. | Commit state, keep raw logs forever, or omit evidence. | Public docs are less directly replayable. |
| Documentation responsibility | Separate current contracts, runbooks, decisions, incidents, tuning, ops logs, plans, and legacy; keep changing state on one current-status page. | v2 ops logs mixed evidence, guidance, procedures, and plans. | Keep judgment in chronological logs. | More maintenance; history is distinguishable from current rules. |
| Runbook and taxonomy | Require third-party dry runs for runbooks; name failures by owner, detection, action, and evidence. | A runbook only its author can run is not mature; `FAIL` is too vague. | Author-only validation and generic failure labels. | More review and vocabulary, faster safer triage. |
| Public CI | Keep CI small and non-mutating. | Prove snapshot integrity without secrets, real RTMPS, or cluster mutation. | Live tests and `k3s apply` in CI. | Fewer production behaviors proven in CI. |

## Review Focus

The highest-signal questions for a reviewer:

1. Are delivery, observation, source data, publication, and recovery ownership
   boundaries clear, and does the media route keep working when monitoring is
   down?
2. Do destructive actions require fresh, consistent, identity-preserving
   evidence?
3. When a design was not finished, was it replaced or deferred with stated
   reasons rather than shipped with guessed thresholds?
4. Are dashboard failures cross-checked against raw metrics before they become
   incidents?
5. Do SLO decisions say what changed and what intentionally did not?
6. Are the docs and tests honest about what was not observed or cannot be
   validated without production access?

## Related Docs

- System: [architecture](architecture.md), [current status](current-status.md),
  [runtime contract](runtime-contract.md), [evolution](evolution.md)
- Media route: [self-healing OCI fan-out](v3/self-healing-oci-fanout.md),
  [route simplification decision](v3/oci-route-simplification-decision.md),
  [FIFO connection recovery](v3/ffmpeg-fifo-recovery.md)
- Recovery authority: [scoped recovery authority](v3/scoped-recovery-authority.md),
  [local reflex authority](../recovery-control/docs/local-reflex-authority.md),
  [v3 decisions](v3/decisions.md)
- YouTube: [lifecycle safety](v3/youtube-lifecycle-safety.md),
  [same-URL evidence](same-url-continuity-evidence.md)
- Observation: [observability](observability.md),
  [SLI methodology](sli-methodology.md), [SLI and dashboard](v3/sli-and-dashboard.md),
  [visual/audio health](v3/visual-audio-health-model.md),
  [notification and auto-recovery](v3/notification-and-auto-recovery.md),
  [failure taxonomy](v3/failure-taxonomy.md)
- Case studies: [operational case studies](operational-case-studies.md),
  [TCP stall](v3/tcp-stall-case-study.md),
  [encoder upload](v3/encoder-upload-case-study.md),
  [memory guard](v3/memory-guard-case-study.md),
  [migration cutover](v3/migration-cutover-case-study.md),
  [map production cutover](v3/map-production-cutover-case-study.md),
  [single-node DR](v3/single-node-dr-case-study.md)
- Review aids: [implementation review map](implementation-review-map.md),
  [test strategy](test-strategy-and-safety-boundary.md),
  [operational scorecard](operational-scorecard.md),
  [runbook validation](v3/runbook-validation.md)
