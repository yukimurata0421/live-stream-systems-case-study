# Recovery Safety Model

## No-action posture

The public policy keeps `production_behavior_change_authorized=false`. The
no-action runtime evaluates evidence but forces a
`CRA_OPERATING_MODE_NO_ACTION` blocker, records hypothetical decisions, and
exposes zero command-delivery and physical-effect capability.

## Authorization gates

A confirmed TCP-stall candidate must pass all required monitoring checks:

```text
tcp_stall = CONFIRMED
network_down = FALSE
ffmpeg_present = TRUE
target_stable = TRUE
maintenance = FALSE
delivery_bad = TRUE
```

The central layer then checks projection freshness, readiness, hard cooldown,
hourly and daily budgets, unresolved commands, restore state, authority epoch,
authorization lifetime, and prior effect-scope reservations.

The runtime repeats independent admission checks for producer identity,
producer generation, request lifetime, exact target identity, unresolved
effects, and logical-generation fences. Target identity is checked again at
the physical boundary.

## Effect identity

The request ID is not the exactly-once key. Two IDs can still refer to the same
physical target generation.

- `logical_generation_scope_id` excludes PID and fences a managed generation.
- `effect_scope_id` binds the action to the full exact target, including PID.
- a unique logical-generation index and `physical_attempt_count <= 1` enforce
  at-most-one physical effect across duplicate requests and process restart.

## Failure handling

- `network_down`, upload pressure, healthy state, startup transients, and
  unavailable targets result in no action.
- an unsafe missing-child state is escalation evidence, not permission for an
  automatic Pod or host restart.
- `OUTCOME_UNKNOWN` is not retried automatically under a new request ID.
- missing evidence, observer failure, harness failure, and SUT failure remain
  distinct outcomes.
- a signed payload with forbidden action, authorization, budget, cooldown, or
  verdict fields is rejected even if its signature is otherwise valid.

## Claim boundary

Passing deterministic tests proves the repository contracts exercised by
those tests. It does not prove production credentials, a live network path, an
elapsed soak, external viewer recovery, or authorization to enable actions.
