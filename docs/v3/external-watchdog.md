# An External Watchdog for the Observation Owner

The local notification health process can report a failed delivery gateway, but
it cannot report the loss of its own host or internet connection. The external
watchdog moves absence detection and delivery into OCI managed services.

```mermaid
flowchart LR
  A[Arena: fresh local observations] -->|heartbeat| M[OCI Monitoring]
  M -->|absence alarm| N[OCI Notifications]
  N --> S[Existing critical Slack destination]
```

Neither the media relay VM nor the local notification gateway forwards the alarm.
The watchdog has no recovery action, stream credentials or media-control authority.
The existing media route, fallback policy, SLO definitions and incident owners remain.

## Evidence and time

The sender checks the notification producer's original boot/boottime, the gateway's
original boot/boottime, and local Prometheus readiness. Copying an old JSON file
cannot renew these observations. Missing, malformed, future or previous-boot
observations withhold the heartbeat.

The metric API requires a UTC timestamp and a dated signature. The sender obtains
them from a fresh TLS response from the fixed OCI ingestion endpoint. Redirects,
cached responses with nonzero age, invalid Date values and excessive round-trip
time are rejected. Local evidence is checked again after this exchange. The official
OCI SDK signs the exact request; HTTP success with partial metric rejection is
still a failed heartbeat. Compressed replies are decoded before bounded parsing.
This avoids subtracting another host's wall clock from the local wall clock.
TLS trust and provider time remain dependencies; a failed clock exchange stops
heartbeats and is reported as unavailable monitoring evidence.

## Alert contract

The current operated constants are recorded in [current status](../current-status.md).
The query selects a stable monitor dimension and uses `groupBy(monitor).absent(...)`.
The alarm starts disabled and is activated only after baseline data and an active
notification subscription have been verified. The pending condition, aggregation,
evaluation slack and notification delivery all contribute to elapsed detection time.

OCI eventually resets an absent stream after its configured absence-detection
period and internal reset interval. **RESET is not recovery.** The message body
says so explicitly. Fresh heartbeat data plus `FIRING_TO_OK` establishes resumed
observation; an OK label alone does not. See Oracle's
[alarm reset behavior](https://docs.oracle.com/en-us/iaas/Content/Monitoring/Concepts/monitoringoverview.htm).

## Authority and deployment

A dedicated compartment, identity, group and policy isolate the sender. Its only
permission is `METRIC_WRITE` for `stream_v3_watchdog`; it cannot read metrics,
change alarms or operate compute resources. API-key use is enabled while console
login and other credential capabilities are disabled. The key is generated on
the observation host and passed through systemd credentials. No administrator
key is copied to that host. The Slack destination is registered in OCI and its
subscription is confirmed before activation.

The immutable release contains the sender and shared local-evidence reader.
`ops/scripts/requirements-oci-watchdog.txt` describes the isolated SDK environment;
`ops/systemd/stream-v3-oci-watchdog.service.in` and its timer bound runtime resources.
Only the new sender is started. The existing monitor, gateway and media processes
are not restarted. Rollback disables the owned cloud alarms before stopping the
new timer; it does not rewind observations, outboxes, incident state or SLO history.

## Validation and limits

A separate validation series uses the same sender, credentials, alarm query and
Slack topic. Pausing only that series exercises real cloud absence detection while
the production heartbeat continues. Receiver messages and alarm transitions are
separate evidence from a successful metric API request. Operated results are kept
in [current status](../current-status.md); no receiver result is inferred from local tests.

Native tests cover the official signature, stale/malformed local evidence, cloud
time failures, redirects, partial acceptance, oversized and compressed responses.
The initial deployment exposed a compressed-response parsing bug; its failed
attempt was retained and a regression added before enabling the alarm.

This arrangement is independent of the home host and home network, but shares
an OCI provider and region with the media relay. A correlated provider outage
remains a limit. Long-duration absence reset, human acknowledgment, continuous
viewer audio/video and provider failover are separate qualifications. The managed
resources fit a small free-tier workload; final billing is not established here.
