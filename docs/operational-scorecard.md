# Operational Scorecard

For a hiring review, read this summary first. The detailed reference table below
is available when a specific claim needs investigation. Dates describe retained
evidence windows, not current service-health guarantees.

## Results At A Glance

| Result | Evidence | Limit |
| --- | --- | --- |
| Same-process output recovery | [FIFO measurements](v3/ffmpeg-fifo-measurements.md): repeated NVENC/AAC trials and corrected 12-second live exercise with observed ACK recovery around 1.95 s. | Initial rollout lost audio; public-player continuity and the full retry-exhaustion/restart cascade remain untested. |
| 160-day same-URL observation | October 14 checkpoint; `140 / 140` ledger dates and zero recorded transitions. | Identity checks do not prove uninterrupted frames or all intraday states. |
| Diagnosed network and watchdog faults | Independent network checks and correlated restart logs. | Router/carrier ownership and the entire reception history remain unproven. |
| Corrected one Twitch renewal exercise | New Twitch session, sampled video/audio, preserved YouTube handler after the correction. | The first exercise failed; later scheduled renewals and long reception soak are separate. |
| Repaired current warnings and public freshness | Recent raw/monitoring agreement and fresh public snapshots after cache expiry. | Historical gaps and formal UNKNOWN results were preserved. |
| Public source checks | Focused and component checks in [validation](../publication/validation.json). | Source tests do not establish production activation or native qualification. |
| October 13 qualification campaign remained unverified | Harness result artifacts were not recovered. | This campaign is separate from the later migration-system runs; missing results are not a pass. |
| One automatic fallback and return drill | Fallback started 52 seconds after a manual relay stop; it later stopped after sustained relay health. | The operator restarted the relay. Other failure modes and frame-level continuity were not tested. |

Read [the incident stories](operational-case-studies.md) for symptoms, actions,
verification, and limits. Read [the continuity note](same-url-continuity-evidence.md)
for exact dates and counting periods.

## Scope Calibration

This is a small, single-operator home streaming system with an OCI relay and a
public static status site. Its evidence concerns operating discipline and
bounded recovery. It does not establish a commercial service's SLO, multi-tenant
scale, or recovery from disk loss and power failure.

Production recovery uses local, bounded actors: in-Pod fast recovery, the
relay's own restart, the forward keeper, the fallback controller, and guarded
restart requests from the monitor. The public CRA policy enables no production
authority. One drill with a stopped relay service exercised the fallback;
the remaining unobserved items are listed in
[current status](current-status.md).

## Detailed Reference

The tables below separate components that govern the current system from
dated evidence gathered earlier.

## Status Legend

| Status | Meaning |
| --- | --- |
| Measured | Observed in a retained operational window or drill. |
| Tested | Covered by public automated tests or non-mutating scripts. |
| Documented | Procedure or boundary exists, but public evidence is not a measured drill. |
| Not publicly measured | Known gap or private-only evidence not included in the public snapshot. |

## Core Claim IDs

These stable identifiers let documentation tests protect the public claims
without freezing their prose or section layout.

| Claim ID | Contract |
| --- | --- |
| `SV3-SAME-URL` | Recoverable faults preserve the current YouTube watch URL. |
| `SV3-RECOVERY-GUARD` | Monitoring evidence cannot bypass scoped recovery authority. |
| `SV3-PUBLIC-BOUNDARY` | Public status is an allowlisted static snapshot, not private monitoring access. |
| `SV3-EVIDENCE-STRENGTH` | Restart observation and confirmed send recovery remain distinct states. |

## Scorecard

### Current Components

| Area | Status | Public evidence | Residual risk |
| --- | --- | --- | --- |
| Same-watch-URL preservation | Measured / tested | `docs/same-url-continuity-evidence.md`, `docs/v3/youtube-lifecycle-safety.md`, resolver freshness tests | The October 14 checkpoint spans at least 160 days of URL observation and has `140 / 140` later-ledger checkpoint dates. The September `127-day / 107-date` record is retained as history. Neither establishes uninterrupted frames; the exact V2-to-V3 authority handoff was not logged separately. |
| Relay/fallback clock observation | Tested / dated short measurement | [Clock contract](v3/relay-clock-monitor.md), [October 22 evidence](v3/public-source-port-2026-10-22.md), paired-OCR, capture, binding and cache tests | Returned video and native forward counters do not establish public reception. The measured normal window was 90 seconds; current gaps are listed in current status. |
| OCI files-only restore | Tested / documented | [Restore procedure](v3/oci-backup-and-restore.md), archive/authentication/placement tests | No new VM bootstrap or live recovery is established by the source port. |
| Guarded restart gates | Tested | `src/watchers/decision/action_gate.py`, `src/watchers/youtube_health.py`, `ops/scripts/v3_shadow_acceptance.py`, `tests/test_v3_shadow_acceptance.py`, `src/stream_v2/recovery_orchestrator/gate.py` | These gates govern the arena-server monitor's restart requests in production. Public tests do not perform live mutation. |
| Scoped recovery authority | Tested / documented | `docs/v3/scoped-recovery-authority.md`, `src/stream_core/runtime_boundary_entrypoint.py`, `tests/test_fast_recovery_runtime_boundary.py` | Public tests use test-owned child processes; they do not enable authority or restart a live FFmpeg process. |
| Central Restart Authority | Tested / documented / production-disabled | `recovery-control/docs/why-cra.md`, `recovery-control/docs/architecture.md`, `recovery-control/docs/harness-trust.md`, `recovery-control/tests/` | No production authority is enabled, and the media route does not use CRA; the snapshot makes no live effect or completed production-soak claim. |
| Monitoring v4 arena evidence plane | Tested / documented / partly in production | `monitoring-v4/src/`, `monitoring-v4/tests/`, `monitoring-v4/docs/architecture.md`, `monitoring-v4/docs/status.md` | The relay observer and public relay projection run in production as read-only components. The other phases remain gated by `monitoring-v4/docs/status.md`, and no part grants recovery authority. |
| Public CI safety | Tested | `.github/workflows/public-snapshot-check.yml`, `docs/test-strategy-and-safety-boundary.md` | CI proves snapshot safety, not production liveness. |
| k3s manifest and shadow behavior | Tested | `ops/scripts/validate_k3s_manifests.py`, `ops/scripts/v3_shadow_acceptance.py`, `tests/test_v3_k3s_preflight.py` | Does not prove production `kubectl apply` on the real cluster. |
| Public status snapshot boundary | Documented | `docs/v3/public-status-snapshot.md`, <https://yukimurata0421.dev/> | The public site is a current sanitized snapshot, not a substitute for retained SLI windows or raw private observability. |
| Public publisher source boundary | Tested / documented | `ops/public-publisher/`, `docs/v3/public-publisher-boundary.md`, publisher tests | The repository validates redaction, last-good, and command construction; it does not upload a real object or prove the live timer is using this source. |
| Observability self-check / dashboard `No data` boundary | Tested / documented | `docs/v3/observability-plane-self-check.md`, `ops/scripts/stream_v3_health_snapshot.py`, `ops/scripts/stream_v3_monitoring_watchdog.py`, `tests/test_stream_v3_health_snapshot.py`, `tests/test_stream_v3_monitoring_watchdog.py` | Snapshot fallback and watchdog metrics preserve visibility; they are not new uptime claims. |
| Compliance and licensing boundary | Documented | `docs/compliance-and-licensing-boundary.md`, `docs/v3/music-provider-and-loudness-contract.md`, `docs/security-and-secrets.md`, `docs/public-release.md` | The repository traces the Floracore permission condition into credit and activation controls, but does not publish or independently prove the private correspondence. This remains an operator risk posture, not legal advice; laws, platform rules, and provider terms can change. |
| Gateway observation safety | Measured / documented | `docs/v3/connectivity-aware-delivery-recovery.md`, `docs/v3/failure-taxonomy.md` | A systemd sandbox once caused a false-negative route observation until `AF_NETLINK` was allowed. The public schema still cannot always distinguish route-command error from a missing gateway, so ambiguous samples must not authorize recovery. |
| YouTube lifecycle mutation safety | Tested / documented | `docs/v3/youtube-lifecycle-safety.md`, cache freshness tests, broadcast selection tests | Real YouTube mutation remains outside public CI. |

### Earlier v3 Evidence (Dated)

These rows record measurements and tests from before the 2026-10-20 route.
Follow each link before applying a result to the current route.

| Area | Status | Public evidence | Residual risk |
| --- | --- | --- | --- |
| Rolling SLI feedback | Documented | `docs/v3/rolling-sli-error-budget-feedback.md`, `docs/v3/sli-and-dashboard.md` | Rolling 24h/7d/available-30d windows are operator feedback. They do not replace the 14-day baseline or 28-day same-URL case study. |
| Durable reliability and external evidence | Tested / documented | `docs/v3/operational-reliability-and-external-evidence.md`, operational reliability and external evidence tests | The public repository proves evidence classification and alert boundaries, not current GCP check health or a live deployment. |
| Fast-recovery restart classifier replay | Tested / measured by replay | `docs/v3/fast-recovery-classifier-replay.md`, `tests/test_sli_pipeline_rotation.py`, `tests/test_subsystems.py` | Replay proves current classifier coverage over retained events, not past executor intent or live production execution. |
| Encoder and upload contract | Measured / tested; source-time deployment not claimed | `docs/v3/encoder-upload-case-study.md`, `docs/v3/encoder-fps-tuning-2026-05-31.md`, `docs/runtime-contract.md`, source-time TCP sample tests | The July 28 temporary 2500k incident was corrected to the normal 5fps/3400k contract. The first source-time rollout was rejected and rolled back; the corrected candidate passed local and passive-shadow checks but was not deployed at the retained checkpoint. Older logs remain uncorrected, and short trials do not replace long-window YouTube input monitoring. |
| Track-free renderer isolated soak | Measured / tested / documented | `docs/v3/map-production-cutover-case-study.md`, `docs/v3/map-rendering-and-monitoring.md`, map and precipitation tests | The accepted 24-hour window measured the isolated browser and renderer, not audio, RTMPS upload, or YouTube playback. |
| Renderer NVENC soak | Measured / tested / documented | `docs/v3/map-production-cutover-case-study.md`, `docs/v3/encoder-upload-case-study.md`, encoder contract tests | The one-hour encode used a local null muxer and was video-only; it was not production delivery evidence. |
| Renderer production cutover | Measured / tested / documented | `docs/v3/map-production-cutover-case-study.md`, `docs/v3/map-rendering-and-monitoring.md`, runtime and map contract tests | The accepted production observation was one hour. Viewer probes and expanded alerting were later follow-ups, and long-window SLI review remains separate. |
| Map mobile-legibility proposal | Locally measured / tested / not deployed | `docs/v3/map-mobile-legibility-review.md`, map and overlay contract tests | Aggregate luma and scaled-frame review support the proposal; no live rollout or universal phone-text readability is claimed. |
| Upload budget | Measured / documented | `docs/v3/encoder-upload-case-study.md`, `docs/v3/sli-and-dashboard.md` | NVENC CBR runs closer to the 5.0 Mbps warning ceiling than the v2 CPU path. |
| TCP stall root-cause split | Measured / tested / documented | `docs/v3/tcp-stall-case-study.md`, `docs/v3/tcp-stall-resolution-depth.md`, observer scripts, `tests/test_wan_observer_scripts.py` | Public repo keeps the observer code, but excludes raw private JSONL samples, exact public IPs, real CPE logs, generated packet metadata, and packet captures. |
| Visual correctness | Documented / tested by components | `docs/v3/visual-audio-health-model.md`, stream watchdog tests | Viewer-visible defect seconds are not directly measured in the public baseline. |
| Audio correctness | Documented / tested by components | `docs/v3/visual-audio-health-model.md`, Pulse/audio tests | Track transitions can create noisy low-energy samples and require staged interpretation. |
| Memory guard | Documented / tested by components | `docs/v3/memory-guard-case-study.md`, memory/resource tests | Short Xvfb peaks can still be hard to capture if they happen between observations. |
| Notification quality | Documented / tested by replay contracts | `docs/v3/notification-and-auto-recovery.md`, notification tests | Notification delivery is secondary SLI, not proof of stream health. |
| Single-node k3s service restart | Measured | `docs/v3/single-node-dr-case-study.md`, `docs/v3/sli-and-dashboard.md` | 10.7s measured to stream_v3 observability metrics OK; same FFmpeg PID/TCP socket continued sending. This is not node reboot, disk restore, RTMPS reconnect RTO, or readsb/tar1090 source recovery. |
| Viewer-facing burn during k3s restart drill | Measured by public/ingest samples | `docs/v3/single-node-dr-case-study.md`, `docs/v3/sli-and-dashboard.md` | YouTube ingest, public watch, same-URL, and watchdog metrics stayed OK; sampling does not prove every delivered frame. |
| OS reboot / power loss / disk loss DR | Documented / not publicly measured | `docs/v3/single-node-dr-case-study.md` | RTO/RPO depend on private state, secrets, music backup, and hardware recovery. |
| 24-hour full production smoke test | Documented gate | `docs/test-strategy-and-safety-boundary.md`, `docs/v3/migration-cutover-case-study.md` | The measured 24-hour renderer soak was isolated and must not be relabeled as a full live-production window. A full smoke test is still a migration gate, not a replacement for 14-day or 28-day SLI review. |
| Runbook third-party validation | Documented | `docs/v3/runbook-validation.md` | Public repo cannot validate private credentials or real production mutation. |
| Three-host source consolidation | Tested / documented / not deployed | `docs/v3/three-host-source-consolidation.md`, host-role and publisher tests | Source ownership is consolidated locally; no host service, Pod, timer, cloud object, or public site was changed. |

## Smoke-Test Position

The recommended live smoke-test window for v3-impacting runtime, encoder, or
recovery changes is 24 hours. The rationale is narrow:

- v2 already provided the long-running behavior baseline for the delivery
  model;
- v3 should prove that migration, k3s ownership, NVENC contract, watchdogs, and
  public evidence still behave across one daily cycle;
- a 24-hour window is long enough to cross routine monitoring, playlist,
  YouTube quota-day, and WAN/session timing boundaries;
- it is short enough to avoid pretending that a single migration gate proves a
  long-window SLO.

After a 24-hour smoke test, the system still needs normal 14-day or 28-day SLI
review before making broad reliability claims.
