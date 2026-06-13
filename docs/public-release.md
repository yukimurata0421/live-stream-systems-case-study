# Public Release Notes

This repository is a sanitized public snapshot of a personal 24/7 ADS-B
streaming system. The goal is to show the architecture, code, tests, deployment
contracts, and operational decisions without publishing runtime data or secrets.

## What Was Kept

- stream_v3 delivery-plane code: rendering, PulseAudio, AutoDJ, FFmpeg/NVENC,
  runtime guards, and k3s entrypoints.
- observability-plane code: YouTube resolver, watchdogs, recovery orchestrator,
  SLI summaries, Prometheus exporter, `ops/monitoring` Prometheus/Loki/Grafana
  config, and observability monitor systemd units.
- v2 historical context and runbooks that explain why v3 exists.
- k3s manifests for shadow, streaming, observer, reports, and cutover gates.
- Tests for config contracts, recovery policy, watchdog behavior, and k3s
  manifest validation.
- Public review docs for executive summary, operational scorecard, test safety
  boundary, incident review, and migration cutover reasoning.
- Sanitized prodesk monitoring extracts in `ops/prodesk-monitoring/`.

## What Was Excluded

- `.state/` runtime state, incident snapshots, local logs, screenshots, and
  capture outputs.
- Packet captures and generated packet-metadata artifacts.
- `ncs_music/` and other local media payloads.
- Virtual environments, Python caches, and generated runtime directories.
- Real YouTube stream keys, OAuth tokens, Discord webhooks, SSH keys, and
  environment files from production state.

## Safety Rules

- Treat every `*.env.example` as a template only.
- Keep production-like values in local untracked files or Kubernetes Secrets.
- Run the secret scan before pushing a public branch.
- Keep public CI non-mutating. Live YouTube mutation and production k3s apply
  belong to explicit local operations, not the public snapshot workflow.
