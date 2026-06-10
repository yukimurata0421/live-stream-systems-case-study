# Documentation

The public documentation is written in English and optimized for review by
interviewers and engineers who want to understand the system quickly.

Start with the top-level `README.md`. Most reviewers should then read only:

1. `hiring-reviewer-guide.md`
2. `executive-summary.md`
3. `operational-scorecard.md`

## Review Paths

### Compact Technical Pass

- `executive-summary.md`
- `operational-scorecard.md`
- `implementation-review-map.md`

### Architecture Pass

- `architecture.md`
- `physical-topology.md`
- `runtime-contract.md`
- `v3/public-status-snapshot.md`

### Reliability / SRE Pass

- `v3/sli-and-dashboard.md`
- `sli-methodology.md`
- `28-day-same-url-sli-case-study.md`
- `v3/tcp-stall-case-study.md`
- `v3/scoped-recovery-authority.md`
- `v3/single-node-dr-case-study.md`

### Deep Design Review

- `design-decisions-for-review.md`
- `v3/decisions.md`
- `implementation-review-map.md`

For validation and incident-review boundaries, read
`test-strategy-and-safety-boundary.md` and `incident-review-template.md`.
Use `00_INDEX.md` as the full documentation catalog.

The original private documentation contained detailed Japanese incident logs and
routine checks. Those logs are not part of the public snapshot because they were
too environment-specific and too noisy for a reusable repository.
