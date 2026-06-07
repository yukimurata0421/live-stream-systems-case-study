# Implementation Review Map

This map is for reviewers who want to connect the public reliability claims to
code and tests without reading the entire repository.

## High-Signal Review Paths

If you only have time for a short code review, start with these files:

- `src/watchers/decision/evaluator.py`
- `src/watchers/decision/action_gate.py`
- `src/watchers/evidence/ledger.py`
- `src/stream_v2/recovery_orchestrator/gate.py`
- `ops/scripts/v3_shadow_acceptance.py`
- `tests/test_youtube_evidence_decision.py`
- `tests/test_cli_systemctl_flow.py`

| Review question | Code | Tests | Docs |
| --- | --- | --- | --- |
| What prevents unsafe staged recovery? | `src/stream_v2/recovery_orchestrator/gate.py`, `src/watchers/decision/*` | `tests/test_v3_shadow_acceptance.py`, `tests/test_action_plan.py`, `tests/test_youtube_evidence_decision.py` | `docs/v3/youtube-lifecycle-safety.md`, `docs/design-decisions-for-review.md` |
| How is same-URL preservation protected? | `src/watchers/youtube_video_id_resolver.py`, `src/watchers/video_resolver/*`, `src/watchers/youtube_api.py` | `tests/test_youtube_broadcast_selection.py`, `tests/test_youtube_video_id_resolver.py`, `tests/test_youtube_monitor_e2e.py` | `docs/28-day-same-url-sli-case-study.md`, `docs/v3/youtube-lifecycle-safety.md` |
| How are stale caches prevented from authorizing bad decisions? | `src/watchers/video_resolver/cache.py`, `src/watchers/youtube_watchdog_core/cache.py` | `tests/test_youtube_video_id_resolver_cache_freshness.py`, `tests/test_youtube_watchdog_cache_freshness.py`, `tests/test_youtube_watchdog_checked_timestamps.py` | `docs/v3/youtube-lifecycle-safety.md` |
| How is RTMPS TCP stall diagnosed? | `src/watchers/fast_recovery_core/decision.py`, `src/watchers/fast_recovery.py`, `ops/scripts/wan_address_observer.py`, `ops/scripts/persistent_tcp_anchor_observer.py`, `ops/systemd/stream-v3-wan-address-observer.timer`, `ops/systemd/stream-v3-persistent-anchor-observer.service` | `tests/test_fast_recovery.py`, `tests/test_wan_observer_scripts.py`, `tests/test_network_observer.py` | `docs/v3/tcp-stall-case-study.md` |
| How are historical fast-recovery stream restarts replayed? | `src/stream_v2/source_reader.py`, `src/stream_v2/subsystems/local_delivery/*`, `src/stream_v2/recovery_orchestrator/proposer.py`, `src/stream_v2/sli.py` | `tests/test_subsystems.py`, `tests/test_orchestrator.py`, `tests/test_sli_pipeline_rotation.py` | `docs/v3/fast-recovery-classifier-replay.md` |
| How is upload tuning decided? | `src/stream_core/engine/ffmpeg_args.py`, `src/stream_core/recovery_profile.py`, `ops/scripts/stream_v3_prometheus_exporter.py` | `tests/test_runtime_contract.py`, `tests/test_stream_v3_prometheus_exporter.py`, `tests/test_docs_structure.py` | `docs/v3/encoder-upload-case-study.md`, `docs/v3/encoder-fps-tuning-2026-05-31.md` |
| How are visual and audio faults kept local? | `src/watchers/stream_watchdog.py`, `src/watchers/stream_watchdog_core/*`, `src/watchers/local_health/*` | `tests/test_stream_watchdog_config.py`, `tests/test_subsystems.py`, `tests/test_runtime_bootstrap_contracts.py` | `docs/v3/visual-audio-health-model.md`, `docs/v3/failure-taxonomy.md` |
| How is memory pressure interpreted? | `src/stream_core/stream_engine.py`, `src/stream_core/cli_support/memory_status.py`, `src/stream_core/cli_support/resource_memory.py` | `tests/test_stream_engine_wait_modes.py`, `tests/test_memory_status.py`, `tests/test_resource_memory.py` | `docs/v3/memory-guard-case-study.md` |
| How is notification noise controlled? | `src/stream_core/notifications/*`, `src/watchers/stream_watchdog.py` | `tests/test_operational_replay_contracts.py`, `tests/test_critical_helper_contracts.py`, `tests/test_cli_ops_commands.py` | `docs/v3/notification-and-auto-recovery.md` |
| How is public CI kept non-mutating? | `.github/workflows/public-snapshot-check.yml`, `src/stream_core/cli.py`, `ops/scripts/v3_shadow_acceptance.py`, `ops/scripts/validate_k3s_manifests.py` | `tests/test_cli_systemctl_flow.py`, `tests/test_v3_shadow_acceptance.py`, `tests/test_v3_k3s_preflight.py`, `tests/test_docs_structure.py` | `docs/operations.md`, `docs/public-release.md`, `deploy/k3s/README.md` |
| How are ADS-B publication and NCS attribution treated as design boundaries? | `src/stream_core/overlay_server.py`, `ui/overlay/index.html`, `.gitignore` | `tests/test_overlay_server_outline.py`, `tests/test_docs_structure.py` | `docs/compliance-and-licensing-boundary.md`, `docs/security-and-secrets.md`, `docs/public-release.md` |
| How is single-node DR scoped honestly? | `deploy/k3s/*`, `src/stream_core/supervisor/*`, `ops/systemd/*` | `tests/test_v3_k3s_preflight.py`, `tests/test_runtime_supervisor.py`, `tests/test_env_sync.py` | `docs/v3/single-node-dr-case-study.md` |
| How is v2 to v3 cutover authority scoped? | `deploy/k3s/*`, `ops/scripts/v3_shadow_acceptance.py`, `src/stream_v3/*` | `tests/test_v3_shadow_acceptance.py`, `tests/test_stream_v3_control_loop.py`, `tests/test_v3_k3s_preflight.py` | `docs/v3/migration-cutover-case-study.md`, `docs/test-strategy-and-safety-boundary.md` |
| What does a 24-hour smoke test prove? | `ops/scripts/v3_shadow_acceptance.py`, `ops/scripts/stream_v3_prometheus_exporter.py`, `src/watchers/*` | `tests/test_docs_structure.py`, `tests/test_v3_shadow_acceptance.py`, cache freshness tests | `docs/operational-scorecard.md`, `docs/test-strategy-and-safety-boundary.md` |
| How are incidents reviewed without leaking private evidence? | `src/watchers/*`, `src/stream_core/notifications/*`, `ops/scripts/*observer*.py` | `tests/test_docs_structure.py`, policy and observer tests | `docs/incident-review-template.md`, `docs/v3/failure-taxonomy.md` |

## What Not To Infer

This repository is a public case study. It intentionally does not prove:

- that another environment can run the system unchanged;
- that public CI performs live YouTube mutation;
- that raw production logs, state files, music, or secrets are present;
- that single-node k3s is an ideal reliability architecture;
- that all RTO/RPO branches have been measured.

The strongest review path is to evaluate the boundaries: what the system
claims, what it refuses to claim, and which tests enforce those boundaries.
