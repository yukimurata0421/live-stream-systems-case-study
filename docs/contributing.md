# Contributing

Contributions are welcome when they improve the public repository as an
engineering case study.

## Good Contributions

- clearer documentation
- safer examples
- validation improvements
- additional tests
- portability notes
- observability examples
- small bug fixes with focused scope

## Pull Request Checklist

- Explain the operational reason for the change.
- Keep the delivery-plane / observability-plane split clear.
- Run `python3 ops/scripts/validate_k3s_manifests.py`.
- Run `python3 ops/scripts/v3_shadow_acceptance.py` for runtime or monitoring
  changes.
- Do not commit secrets, media payloads, `.state/`, logs, or local captures.
- Keep examples generic and public-safe.

## Design Changes

Design changes should include a short decision note that covers context,
decision, consequences, evidence, and alternatives considered.
