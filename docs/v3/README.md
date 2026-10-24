# stream_v3 Documentation Folder

This folder holds the v3 contracts, dated case studies, and publication
records. They are listed by topic, with one line each, in the
[documentation catalog](../00_INDEX.md); the
[documentation guide](../README.md) suggests where to start.

The pages most readers need from this folder:

- [FIFO connection recovery](ffmpeg-fifo-recovery.md): why the output reconnects
  instead of riding through or restarting FFmpeg; its
  [measured comparison and rollout failure](ffmpeg-fifo-measurements.md).
- [Self-healing OCI fan-out](self-healing-oci-fanout.md): the media route since
  2026-10-20.
- [Replacing an unfinished OCI recovery design](oci-route-simplification-decision.md):
  why the route was simplified.
- [Return-feed clock monitor](relay-clock-monitor.md): clock, native-forward and
  observer-quality facts; [OCI files-only restore](oci-backup-and-restore.md):
  encrypted backup and offline placement.
- [Twitch thumbnail observation](twitch-thumbnail-monitor.md): slow provider-image
  evidence beside the fast return branch; no player E2E claim.
- [TCP stall case study](tcp-stall-case-study.md): diagnosing a home-network
  fault without replacing the broadcast.
- [YouTube lifecycle safety](youtube-lifecycle-safety.md): what must be true
  before a destructive YouTube action.

Superseded pages open with a banner that names the replacement. Hosts and data
paths are described once in
[architecture](../architecture.md).
