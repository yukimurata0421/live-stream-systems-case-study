# Migration And Cutover Case Study

This case study explains how `stream_v3` treats migration from the v2
single-host runtime to the v3 k3s runtime. The important rule is that a green Pod is not production authority.

## Migration Problem

The v2 system already carried important reliability behavior: same-URL
preservation, watchdog evidence, recovery restraint, quota awareness, and
operational runbooks. Moving to v3 should not erase those properties just
because the delivery process moved into k3s.

The migration therefore had two goals:

- preserve the v2 safety model while changing runtime ownership;
- make the production authority transfer explicit enough that a reviewer can
  tell what owns delivery, monitoring, state, metrics, and recovery.

## Authority Transfer Model

Production authority is transferred only when these surfaces agree:

| Surface | Cutover question |
| --- | --- |
| Runtime owner | Is the intended v3 runtime publishing, and is any older publisher stopped? |
| State root | Are runtime, watchdog, resolver, and recovery state paths pointing at the intended owner? |
| CLI / supervisor | Do operator commands target v3 and record restart attribution? |
| Metrics namespace | Do dashboards read `stream_v3_*` metrics from the intended source labels? |
| Recovery path | Are mutating recovery actions gated and no longer shadow-only? |
| YouTube identity | Is the expected watch URL and video ID preserved? |
| Rollback boundary | Is the reason and authority for returning to v2 explicit? |

This prevents the common migration mistake of treating `3/3 Running` as
evidence that production ownership is complete.

## Phases

| Phase | Goal | Exit signal |
| --- | --- | --- |
| Public config sync | Make examples, runtime docs, and k3s manifests agree. | Docs and config contract tests pass. |
| Shadow validation | Exercise command mapping, state writes, SLI summaries, and action plans without mutation. | Shadow acceptance passes with `execute=false`. |
| Production-like runtime check | Confirm GPU/NVENC, PulseAudio, source map, and runtime prerequisites on the delivery host. | Local checks pass on the intended host. |
| 24-hour smoke test | Prove migrated behavior across one daily cycle. | Same URL preserved, YouTube health acceptable, upload within guardrail, visual/audio/memory/recovery evidence clean. |
| Long-window review | Turn smoke-test confidence into measured SLI evidence. | 14-day or 28-day review with denominators, gaps, and unknowns. |

## 24-Hour Smoke-Test Rationale

The recommended smoke-test window is 24 hours, not because 24 hours proves
long-term reliability, but because it is the right migration gate for this
system.

The basis is that v2 already had stable long-running behavior for the personal
24/7 stream. v3 primarily changes the runtime boundary, k3s ownership, NVENC
contract, observability wiring, and recovery authority. A 24-hour window checks
that those changes survive a full daily cycle:

- routine monitoring and notification cadence;
- AutoDJ and now-playing rotation;
- YouTube quota-day and resolver/watchdog freshness;
- WAN/session timing that may recur around a daily window;
- upload and YouTube input-health behavior under the current encoder contract;
- memory and capture-stack behavior outside a short launch-only test.

The smoke test is therefore a confidence gate before broader reliance on v3. It
is not a substitute for the 28-day same-URL review style used elsewhere in the
public docs.

## Rollback Rule

Rollback is allowed only when the operator records:

- why v3 recovery is not the safer path;
- whether same-URL preservation is still possible;
- which runtime owns the live publisher after rollback;
- which state root, CLI path, and metrics source are authoritative;
- what evidence must be collected before trying v3 again.

Automatic rollback is intentionally avoided. A second publisher or stale state
path can be more damaging than a short, well-classified delivery incident.

## Public Implementation Hooks

- `deploy/k3s/` contains the public k3s manifests and shadow overlay.
- `ops/scripts/validate_k3s_manifests.py` validates manifest structure.
- `ops/scripts/v3_shadow_acceptance.py` validates shadow behavior and action
  blockers.
- `docs/runtime-contract.md` and `docs/v3/current-runtime-contract.md` define
  the current runtime and encoder contract.
- `docs/test-strategy-and-safety-boundary.md` defines public CI and live
  smoke-test boundaries.
- `tests/test_v3_k3s_preflight.py`, `tests/test_stream_v3_control_loop.py`,
  and `tests/test_v3_shadow_acceptance.py` cover the public validation path.

## Review Signal

The review value is the discipline around authority. `stream_v3` does not claim
that Kubernetes itself makes the stream reliable. It claims that migration is
safer when runtime ownership, evidence ownership, mutation authority, smoke
tests, rollback boundaries, and long-window SLI review are named separately.
