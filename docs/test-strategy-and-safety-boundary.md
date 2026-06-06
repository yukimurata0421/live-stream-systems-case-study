# Test Strategy And Safety Boundary

The public test strategy is intentionally non-mutating. It proves that the
snapshot is structurally coherent and that safety gates behave as expected, but
it does not pretend to operate the real production stream.

## Test Layers

| Layer | Purpose | Examples |
| --- | --- | --- |
| Compile and import checks | Catch broken Python and script syntax. | `python -m compileall -q src tests ops/scripts` |
| Documentation structure tests | Keep public review claims, reading order, and sanitized boundaries intact. | `tests/test_docs_structure.py` |
| Unit and policy tests | Validate recovery, resolver, watchdog, notification, memory, and runtime decisions. | `tests/test_youtube_watchdog_cache_freshness.py`, `tests/test_v3_shadow_acceptance.py` |
| Manifest validation | Check k3s objects and public example configuration. | `ops/scripts/validate_k3s_manifests.py` |
| Shadow acceptance | Prove command mapping, state writes, action plans, and blockers without live mutation. | `ops/scripts/v3_shadow_acceptance.py` |
| Observer script tests | Keep diagnostic tools parseable and safe to run as report-only helpers. | `tests/test_wan_observer_scripts.py` |
| Live smoke test | Confirm production-like behavior across a bounded time window. | Manual 24-hour gate after high-impact v3 runtime changes. |

## Public CI Boundary

Public CI may:

- compile code and scripts;
- validate sanitized manifests;
- run shadow acceptance;
- run focused unit and policy tests;
- check documentation structure and public evidence links;
- validate report-only diagnostic script behavior.

Public CI must not:

- publish to YouTube;
- use real stream keys, OAuth tokens, Discord webhooks, SSH keys, or private
  environment files;
- mutate a production k3s cluster;
- apply production manifests;
- delete PVCs or runtime state;
- replace, bind, transition, or delete YouTube broadcasts.

This boundary is part of the safety design. A public repository should not need
private credentials to prove that destructive actions are guarded.

## 24-Hour Smoke Test

For changes that affect the v3 runtime owner, encoder contract, recovery path,
YouTube lifecycle policy, or production cutover authority, the recommended live
smoke-test window is 24 hours.

The rationale is specific:

- v2 already established the stable long-running behavior model for this
  personal 24/7 stream;
- v3 changes the ownership boundary, k3s runtime, and NVENC contract, so it
  must still prove that the inherited behavior survives migration;
- 24 hours crosses one daily cycle of monitoring, playlist rotation, YouTube
  quota-day behavior, WAN/session timing, routine checks, and notification
  cadence;
- the window is long enough to catch common migration regressions but short
  enough to remain a smoke test rather than an exaggerated SLO claim.

The 24-hour smoke test is not a replacement for 14-day or 28-day SLI review. It
is a cutover confidence gate.

## Smoke-Test Pass Criteria

A 24-hour smoke test passes only if the operator can record:

- same watch URL preserved;
- no replacement broadcast created by recovery logic;
- YouTube public/live/ingest evidence remains acceptable or any gap is
  explained;
- RTMPS send samples and upload p95 remain inside the accepted budget;
- no persistent YouTube low-bitrate or not-enough-video warning;
- visual checks show the intended map and overlay;
- audio route and monitor energy remain healthy outside transition grace;
- no unresolved Xvfb/cgroup OOM or memory guard breach;
- recovery actions stay within budget and are attributable;
- notification output separates active incidents from auto-recovered events;
- resolver/watchdog evidence is fresh enough for decisions;
- all residual risks are recorded before broader rollout.

If any destructive YouTube action is needed during the window, the smoke test is
not considered a simple pass. It becomes an incident or cutover review.

## Review Signal

The strongest testing claim in this repository is not test volume. It is the
boundary: automated tests prove safety and policy without needing production
secrets, while live smoke tests and long-window SLI reviews are named as
separate operational evidence.
