# Contributing

This repository is not a general-purpose OSS project. It is a public
engineering case study for a personal 24/7 streaming system.

## Acceptable Scope

Small pull requests may be considered when they improve the public evidence
without turning the repository into a supported product:

- clearer documentation
- safer examples
- validation improvements
- additional tests
- portability notes
- observability examples
- small bug fixes with focused scope

Large rewrites, installer work, generic productization, production environment
support, and feature requests for unrelated deployments are out of scope.

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
