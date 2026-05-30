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
