# Public Source Port: Return-Feed Clock Monitoring And OCI Backup

This port adds the bounded return-feed monitor, its review corrections and the
OCI files-only backup tooling. It adapts the operated implementation into
English, configurable public examples. Copying source into this checkout does
not deploy it, restore a relay, enable a recovery policy or publish private
artifacts.

## Why The Monitor Changed

The first version compared the clock region's pixels and audited OCR only
occasionally. The archived pre-change history contained 127 rows,
including 110 probable-progress rows, 14 OCR-confirmed rows and three warmup
rows. It also contained two duplicate cycle identities. The exporter treated probable and confirmed progress the
same way. That was too strong a claim: changing pixels cannot establish that
the clock advanced, and a healthy common relay cannot establish platform
reception.

The correction keeps a continuous decoder, checks both JST and UTC every sample,
and compares clock progress against monotonic elapsed time. An uncertain sample
is quality evidence rather than a direct restart trigger. The
[monitor contract](relay-clock-monitor.md) defines missing-data, delay and
classification semantics; [architecture](../architecture.md#clock-observation-path)
defines the paths.

## Dated Operated-System Evidence

These measurements were obtained before public adaptation. They describe the
stated windows, not a deployment of this public checkout.

| Check | Result | Limit |
| --- | --- | --- |
| Operated OCI return, 2026-10-22 01:42-01:44 JST | Ten unique cycles in 90 seconds confirmed JST/UTC progression. | Short normal window; not public-player reception. |
| Monitor resources in that window | Mean CPU 0.174 core, peak memory about 127 MiB, no OOM. | The fallback producer was inactive; not a worst-case two-input guarantee. |
| Import freshness | Cycle completion to import took 0.802-1.256 seconds; 16 checked new-Pod history rows had no duplicate identity. | Earlier duplicate history was retained. |
| Display age | Valid samples were 3.464-3.722 seconds. | Includes decoding/queues and clock quantization; not network-only RTT. |
| Isolated stopped/blocked receiver test | Final run passed in 106.706 seconds; primary bytes increased through receiver absence, an unread socket, stop and restart; video/audio decode and paired-clock progress recovered. | Same MediaMTX binary, FFmpeg 5.1.9 and backup argument builder, isolated from production. Fixture-wide CPU 2 / memory 768 MiB differs from the observer's 500m limit. |
| ADS-B observation, 01:45:32-01:46:02 JST | Same receiver/decoder processes, no decoder-counter reset; position JSON age at most 0.408 seconds. | No stopped-monitor baseline or controlled RF conditions. One receiver statistics snapshot reported zero lost buffers; this does not prove absence of RF loss. |
| Offline OCI restore bundle | Authenticated decryption and 20-file inventory verified; 12 runtime/network files had previously been placed into an empty review root. | No new-VM bootstrap, service start or provider reception test. |

The final isolation run followed failed attempts. Initial capture did not fit
within the first-cycle connection deadline. A later run exposed continuous UDP
packet loss: the old pipe-input queue and drain batch were too small. Increasing
the bounded queue and drain batch removed those drops. Further samples exposed
ROI-edge and OCR-confidence problems in the synthetic clock. The fixture gained
text margins and recorded a series of quality results; confidence was not lowered
to obtain a pass. Failed attempts were retained privately.

The operated worker used one persistent FFmpeg PID during the 90-second check.
Main and backup Deployment specifications and generations remained unchanged;
the backup stayed at zero replicas. The monitor update did not exercise a live
fallback or change the media route.

## Public Adaptations

| Surface | Public adaptation |
| --- | --- |
| Analyzer and continuous receiver | English comments/docstrings; state names, timing tolerances, capacity limits and control flow retained. |
| Import/native facts | Generic `delivery-host` / `relay-host` identities, separate operator configuration and a dedicated observer account; HMAC, boot, CA, image and producer bindings retained. |
| Exporter | Selective addition to the public exporter, retaining its existing mappings and expensive cache; new observations refresh on each scrape. |
| OCI capture | Root-owned source identity and machine-id binding replace the private host-contract location; private one-time helper and host-contract files are excluded. |
| Backup source archive | Adds the new analyzer/receiver/native-facts dependencies so a new source snapshot does not omit them. |
| Deployment | Generic standalone manifests and Secret/ConfigMap references, with address/image placeholders. No cluster mutation or credential installation. |
| Documentation | Stable design and restore procedure, dated evidence/port record, and links from the existing guide, catalog, architecture and review map. Volatile items stay in current status. |

Source lineage, adaptations, exclusions and local check results are recorded in
[`publication/clock-monitor-port-20261022.json`](../../publication/clock-monitor-port-20261022.json).
The existing [source map](../../publication/source-map.json) retains the digest
of each selected public file. Existing uncommitted Twitch observation work is
preserved separately.

The public repository contains no saved runtime/image archive, passphrase,
HMAC key, private CA material, production settings, raw logs, state databases,
packet captures or private operational receipts. The operated system's private
storage copy is not a public release artifact. See the
[backup runbook](oci-backup-and-restore.md) for the code's file-placement boundary.

## Source Validation

Local checks use synthetic facts, temporary files, mocked transport, finite
local FFmpeg children and offline GPG fixtures. They perform no provider access,
notifications, cluster operations or recovery effects. The machine-binding
adaptation has negative controls for wrong ownership, writable configuration,
symlink, host and machine identity. An exporter-cache regression verifies that
a changed clock result is visible without waiting for the legacy cache to expire.

Exact local results belong to the publication record linked above. Remaining
operational questions and disabled behavior belong to
[current status](../current-status.md), rather than being repeated as current
claims in this dated record.
