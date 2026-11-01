# Thumbnail collector on K3s

This deployment owns provider-thumbnail acquisition and OCR on the monitoring
host. It has no media, recovery, Kubernetes API or Helix credentials. Dell's
stream runtime and local CRA processing do not depend on it.

The existing Helix and relay observers remain host services. The optional
source-adapter listener is a transitional boundary for these observers, not a
migration of the entire monitoring stack.

```text
existing Helix collector -> coherent API + URL bundle --+
existing relay observer -> fixed GET-only adapter ------+-> internal facts Service
                                                          |
                                                      thumbnail Pod
                                                          |
                                                  PVC + publication Service
                                                          |
                                                existing metrics exporter
                                                          |
                                               Prometheus -> public publisher
```

The collector uses Service names and has no hostPath or hostNetwork. Only the
host adapter knows the legacy API/RAM paths and relay loopback endpoint. The
metrics exporter uses the publication ClusterIP, preserving its existing job
labels and history. It never falls back to a frozen host file when HTTP fails.

`manifest.json` defines the namespace, ConfigMap, PVC, Services, EndpointSlice,
Recreate Deployment and NetworkPolicy. `render.py` validates the target identity,
private pod gateway and image digest; it defaults to zero replicas for staged
state transfer. It does not contact Kubernetes. The source-adapter listener must
bind only the pod gateway, with the same address supplied to the EndpointSlice.
The exporter template substitutes the allocated publication Service address and
the original loaded baseline arguments, changing only the overlay entrypoint.

`prepare_context.py OUTPUT` creates a new build directory containing this
Dockerfile and only the imported thumbnail/clock modules under `src/stream_v3/`.
Use that directory as the build context. The image bundles Python,
Pillow, Tesseract, its English data and tini. Import the image into the target
runtime and use its actual manifest digest; a source-derived tag alone does not
pin installed OS packages. No host package changes are needed.

The Pod is pinned to the observation host so the producer and consumer retain
their shared boot identity. It runs as UID/GID 1000, without capabilities or
service-account token, with a read-only root filesystem, 200m CPU / 256Mi memory
limits and a 32Mi memory-backed `/tmp`. Only its PVC is persistent. Application
limits retain two bounded numeric history files and bounded abnormal clock ROIs;
normal full images remain in memory. Keep the bound PV's reclaim policy Retain
and do not delete the PVC during rollback. Local-path storage capacity is not a
filesystem quota or multi-host availability guarantee.

`/healthz` measures monotonic progress of the collector loop, including UNKNOWN
results. Provider failure does not fail readiness/liveness by itself. `/observation`
returns the actual publication with original timestamps; age is never reset by a
reader or proxy. Both are private, GET-only endpoints. The source bridge exposes
only `/bundle` and `/forward`; it has no arbitrary proxy or filesystem API.

For cutover, stop the old worker after a completed cycle, preserve every state
file and verify its hashes after importing into the PVC. Remove the transfer
process before starting the Deployment. Carry forward last-request and provider
cooldown timestamps, verified anchors, counters, startup-grace origins and
history. Recreate and the existing PVC file lock prevent concurrent writers.
During rollback, stop and confirm termination of the Pod, then transfer its
latest state back before starting the old worker. Restoring an initial snapshot
would rewind polling limits and evidence. Preserve prior source/unit/config and
the exact exporter/API drop-ins for a guarded rollback.

The NetworkPolicy bounds DNS, public HTTPS and the facts gateway. It does not
authenticate a hostile node; node administrators remain trusted. Verify its
actual behavior in the target cluster. This remains a single-node observer with
host-service dependencies. It neither repairs a slow provider CDN nor proves
public-player video/audio continuity.

The stable observation contract and dated deployment evidence belong in the
repository documentation; example definitions contain no live addresses,
credentials or production observation data.
