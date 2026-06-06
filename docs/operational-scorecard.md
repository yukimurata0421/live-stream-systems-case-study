# Operational Scorecard

This scorecard separates measured behavior, tested contracts, documented
procedures, and remaining unknowns. It is intentionally conservative: a public
case study is stronger when it says what has not been proven.

## Scope Calibration

This is a single-operator, three-home-host personal 24/7 stream. It has real
live operation, real recovery decisions, retained measurements, and a public
GCS/Cloudflare static edge that offloads public status reads away from the home
uplink, but it is not a commercial multi-tenant service and does not claim a
contractual user SLO. The right reading is reliability discipline at small blast
radius.

## Status Legend

| Status | Meaning |
| --- | --- |
| Measured | Observed in a retained operational window or drill. |
| Tested | Covered by public automated tests or non-mutating scripts. |
| Documented | Procedure or boundary exists, but public evidence is not a measured drill. |
| Not publicly measured | Known gap or private-only evidence not included in the public snapshot. |

## Scorecard

| Area | Status | Public evidence | Residual risk |
| --- | --- | --- | --- |
| Same-watch-URL preservation | Measured / tested | `docs/28-day-same-url-sli-case-study.md`, `docs/v3/youtube-lifecycle-safety.md`, resolver freshness tests | Public numbers are historical windows, not a current uptime promise. |
| Guarded recovery | Tested | `ops/scripts/v3_shadow_acceptance.py`, `tests/test_v3_shadow_acceptance.py`, `src/stream_v2/recovery_orchestrator/gate.py` | Public tests do not perform live mutation. |
| Public CI safety | Tested | `.github/workflows/public-snapshot-check.yml`, `docs/test-strategy-and-safety-boundary.md` | CI proves snapshot safety, not production liveness. |
| k3s manifest and shadow behavior | Tested | `ops/scripts/validate_k3s_manifests.py`, `ops/scripts/v3_shadow_acceptance.py`, `tests/test_v3_k3s_preflight.py` | Does not prove production `kubectl apply` on the real cluster. |
| Public status snapshot boundary | Documented | `docs/v3/public-status-snapshot.md`, <https://yukimurata0421.dev/> | The public site is a current sanitized snapshot, not a substitute for retained SLI windows or raw private observability. |
| Fast-recovery restart classifier replay | Tested / measured by replay | `docs/v3/fast-recovery-classifier-replay.md`, `tests/test_sli_pipeline_rotation.py`, `tests/test_subsystems.py` | Replay proves current classifier coverage over retained events, not past executor intent or live production execution. |
| Encoder contract | Measured / tested | `docs/v3/encoder-upload-case-study.md`, `docs/v3/encoder-fps-tuning-2026-05-31.md`, `docs/runtime-contract.md` | Short trials do not replace long-window YouTube input monitoring. |
| Upload budget | Measured / documented | `docs/v3/encoder-upload-case-study.md`, `docs/v3/sli-and-dashboard.md` | NVENC CBR runs closer to the 5.0 Mbps warning ceiling than the v2 CPU path. |
| TCP stall root-cause split | Measured / documented | `docs/v3/tcp-stall-case-study.md`, observer scripts, `tests/test_wan_observer_scripts.py` | Public repo excludes raw private JSONL samples and exact public IPs. |
| YouTube lifecycle mutation safety | Tested / documented | `docs/v3/youtube-lifecycle-safety.md`, cache freshness tests, broadcast selection tests | Real YouTube mutation remains outside public CI. |
| Visual correctness | Documented / tested by components | `docs/v3/visual-audio-health-model.md`, stream watchdog tests | Viewer-visible defect seconds are not directly measured in the public baseline. |
| Audio correctness | Documented / tested by components | `docs/v3/visual-audio-health-model.md`, Pulse/audio tests | Track transitions can create noisy low-energy samples and require staged interpretation. |
| Memory guard | Documented / tested by components | `docs/v3/memory-guard-case-study.md`, memory/resource tests | Short Xvfb peaks can still be hard to capture if they happen between observations. |
| Notification quality | Documented / tested by replay contracts | `docs/v3/notification-and-auto-recovery.md`, notification tests | Notification delivery is secondary SLI, not proof of stream health. |
| Single-node k3s service restart | Measured | `docs/v3/single-node-dr-case-study.md`, `docs/v3/sli-and-dashboard.md` | 10.7s measured to stream_v3 observability metrics OK; same FFmpeg PID/TCP socket continued sending. This is not node reboot, disk restore, RTMPS reconnect RTO, or readsb/tar1090 source recovery. |
| Viewer-facing burn during k3s restart drill | Measured by public/ingest samples | `docs/v3/single-node-dr-case-study.md`, `docs/v3/sli-and-dashboard.md` | YouTube ingest, public watch, same-URL, and watchdog metrics stayed OK; sampling does not prove every delivered frame. |
| OS reboot / power loss / disk loss DR | Documented / not publicly measured | `docs/v3/single-node-dr-case-study.md` | RTO/RPO depend on private state, secrets, music backup, and hardware recovery. |
| 24-hour production smoke test | Documented gate | `docs/test-strategy-and-safety-boundary.md`, `docs/v3/migration-cutover-case-study.md` | A smoke test is migration confidence, not a replacement for 14-day or 28-day SLI review. |
| Runbook third-party validation | Documented | `docs/v3/runbook-validation.md` | Public repo cannot validate private credentials or real production mutation. |

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
