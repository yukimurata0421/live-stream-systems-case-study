# Design Decisions For Review

This table summarizes the decisions that matter most when reviewing
`stream_v3` as a reliability and platform engineering case study.

| Decision | Why it was chosen | Alternative rejected | Trade-off |
| --- | --- | --- | --- |
| Use k3s for `stream_v3`. | The delivery workload needed a clearer runtime boundary after single-host resource and process ownership contention. | Continue with systemd-only ownership on one host. | Kubernetes adds operational complexity, but makes workload boundaries and dry-run validation explicit. |
| Split delivery and observability. | Monitoring should classify evidence and request recovery without directly owning FFmpeg. | Run delivery, monitoring, and recovery on the same host with shared process ownership. | More machines and coordination, but smaller recovery blast radius. |
| Keep HP ProDesk as the observability owner. | Long-window SLI, dashboard state, and staged recovery decisions should survive delivery-host pressure. | Let the Dell delivery host own all monitoring loops. | Requires remote request plumbing, but prevents local delivery pressure from blinding monitoring. |
| Keep the Airspy/readsb source chain distinct from `stream_v3` delivery. | Airspy on HP ProDesk, `airspy_adsb`, ProDesk readsb, Dell readsb, and the Dell modified tar1090 map endpoint can fail differently from browser rendering or RTMPS delivery. | Treat the ADS-B map endpoint as just another browser/rendering symptom. | Source freshness checks become explicit, but the public k3s manifests do not attempt to own the Airspy device. |
| Stage recovery through guards. | False restarts can damage a live stream more than a short observation delay. | Let watchdogs restart immediately on local symptoms. | Recovery may be slower, but destructive actions require fresher and more consistent evidence. |
| Treat API quota exhaustion as degraded evidence. | YouTube API failure should not automatically imply delivery failure. | Treat API errors as authoritative stream failure. | State classification is more complex, but quota exhaustion is less likely to cause bad recovery. |
| Keep shadow mode before cutover. | The system should prove command mapping, state writes, and action plans before production mutation. | Enable k3s recovery actions directly. | More validation steps, but clearer safety evidence. |
| Keep public CI small. | The public repository should prove the snapshot boundary without secrets, real RTMPS, or cluster mutation. | Run full pytest, live YouTube checks, and k3s apply in CI. | CI proves fewer things, but avoids unsafe or environment-specific checks. |
