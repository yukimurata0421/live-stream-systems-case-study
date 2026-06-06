# Visual And Audio Health Model

YouTube ingest being connected does not prove that viewers see the intended
video and hear the intended audio. An `RTMPS connected` state is transport
evidence, not visual or audio proof. `stream_v3` therefore treats visual and
audio health as separate product signals.

## Visual Health

The stream renders a browser-driven ADS-B map and overlay. A successful RTMPS
connection can still publish bad output if Chromium, Xvfb, the map source, or
overlay metadata is broken.

Visual checks look for:

- ADS-B map content;
- receiver panel;
- aircraft icons and movement/source freshness;
- range outline and labels;
- now-playing overlay;
- footer clock;
- absence of native tar1090 error bands;
- absence of blank or dark frames.

Map-source and movement probes are useful evidence, but they can be noisy. The
system keeps weak visual probes report-only until repeated correlation exists.

## Audio Health

Audio is validated by route and energy, not process liveness alone.

Checks include:

- PulseAudio socket reachability;
- expected sink and monitor source;
- DJ sink input presence;
- FFmpeg capture source-output;
- monitor energy/RMS;
- transition grace during track changes.

The system avoids restarting the whole stream on a single low-energy sample.
Track transitions and heartbeat-only now-playing updates can temporarily look
like silence, so recovery is staged.

## Recovery Boundaries

| Symptom | First response | What it must not do |
| --- | --- | --- |
| now-playing unknown | inspect metadata writer, DJ state, overlay JSON | replace YouTube broadcast |
| map error band | inspect browser source and upstream map path | assume RTMPS failure |
| blank/dark frame | inspect browser/Xvfb/capture path | mutate YouTube lifecycle directly |
| Pulse source missing | repair Pulse route or restart DJ/audio stage | create new watch URL |
| confirmed audio energy low | staged DJ/audio then stream recovery if needed | treat one low sample as outage |
| ADS-B source stale | classify source freshness separately from media delivery | hide it as "stream OK" |

## Public Implementation Hooks

- `src/watchers/stream_watchdog.py` coordinates visual/audio checks.
- `src/watchers/stream_watchdog_core/overlay_health.py` validates ADS-B
  freshness signals.
- `src/watchers/stream_watchdog_core/pulse_routes.py` and
  `pulse_metrics.py` classify audio route and energy.
- `src/watchers/local_health/*` keeps local delivery, rendering, and audio
  actions scoped.
- `src/stream_core/overlay_server.py` owns the map proxy/overlay boundary.
- `tests/test_stream_watchdog_config.py`, `tests/test_subsystems.py`,
  `tests/test_overlay_server_outline.py`, and
  `tests/test_runtime_bootstrap_contracts.py` cover the behavior.

## Review Signal

The important review signal is separation. The system does not collapse "RTMPS
connected," "YouTube public live," "visual correctness," "audio correctness,"
and "ADS-B source freshness" into one vague health bit.
