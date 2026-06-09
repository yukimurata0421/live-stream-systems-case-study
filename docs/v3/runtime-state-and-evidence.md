# Runtime State And Evidence

Runtime state is written outside Git.

Important state paths:

```text
/state/now_playing.txt
/state/overlay/now_playing.json
/state/logs/fast_recovery_events.jsonl
/state/subsystems_status.json
/state/recovery_action_plan.json
/state/objective_sli.json
```

## Evidence Rules

- Fresh local delivery evidence wins over stale dashboard evidence.
- Monitoring unknowns must not authorize destructive delivery actions.
- YouTube lifecycle actions require identity and URL-preservation evidence.
- Audio faults can restart audio or DJ components, but must not create a new
  YouTube broadcast by themselves.
- Shadow mode may build an executable-looking plan, but `execute` must remain
  false and `shadow_mode` must block production mutation.

## Measurement Evidence

The public repository records summarized evidence, not private state payloads.
For the 2026-05-31 encoder fps decision, the retained public evidence is the
trial summary in `docs/v3/encoder-fps-tuning-2026-05-31.md`: upload average,
p95, max, over-budget seconds, YouTube health classification, and the derived
per-frame bit budget for 4fps, 5fps, and 10fps at 3400k video CBR.

For the recurring TCP stall investigation, the retained public evidence is the
diagnostic model in `docs/v3/tcp-stall-case-study.md` and the report-only
observer code in `ops/scripts/wan_address_observer.py` and
`ops/scripts/persistent_tcp_anchor_observer.py`. Raw private JSONL samples and
exact public IP addresses are not retained in Git.

The public observer contract keeps two granularities separate:

- normal samples for long-window context;
- high-cadence burst or failure-triggered samples for short WAN/session
  transitions.

`sample_reason` labels why a WAN sample exists, for example scheduled cadence,
the recurring morning validation burst, or a persistent-anchor failure
follow-up. This lets reviewers distinguish normal background evidence from
incident-window attribution evidence without publishing raw private logs.
