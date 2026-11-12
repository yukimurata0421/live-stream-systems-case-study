# CRA Pod API observer

The delivery host keeps process identity, the signed target snapshot and the
Executor boundary. Only Kubernetes Pod reads move into this separate observer.
The existing collector reads its UNIX socket and still performs two Pod reads,
FFmpeg cgroup/PID/start-tick checks and target consistency validation.

The observer has no Agent DB, effect socket or control command. Its projected
token is reopened for each API request; no host kubeconfig renewal job is needed.
The health client imports neither API nor TLS code. Four socket slots and one
API-fetch slot are separate, so health probes can run while a fetch is busy.
Failures retain a bounded stage/reason/duration diagnostic without token content.

## Build and render without applying

From the repository root, copy the Dockerfile and the two source files into an
owned build directory; the Dockerfile expects both files beside it:

```sh
mkdir -p /tmp/cra-observer-build
cp ops/kubernetes/cra-pod-observer/Dockerfile /tmp/cra-observer-build/
cp ops/selfheal/pod_api_observer.py ops/selfheal/pod_api_health.py /tmp/cra-observer-build/
```

Build and inspect that image using the operator's isolated build procedure.
`render.py` requires its immutable digest and an explicit node name:

```sh
python3 ops/kubernetes/cra-pod-observer/render.py \
  --image 'registry.example.invalid/cra-observer@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
  --node delivery-node > /tmp/cra-observer-deployment.json
```

This renders a Deployment only; it does not apply it. The template has CPU
10m/250m, memory 24Mi/96Mi, UID 992/GID 983, read-only root filesystem, no elevated
capabilities and a 600-second projected token. The image must already exist on
the node (`imagePullPolicy: Never`). Review UID/group membership, token audience,
namespace, node identity, the fixed selector and the socket directory first.
The namespace-scoped `rbac.yaml` is an inert example. Listing Pods exposes Pod
specifications within that namespace; the observer returns only its allowlisted
projection. Do not give it secrets or write access.

`tmpfiles.conf` prepares the socket directory. `collector.conf.template` selects
the existing runner and the adapter explicitly; review the real collector's
mounts, imports and service identity before using it. The public collector
already has the shared FIFO-aware classifier, so this adapter does not import
the historical deployment-specific FIFO overlay or its private base hash.

## Isolated checks

```sh
python3 -m pytest -q tests/test_pod_api_observer.py tests/test_fifo_target_snapshot.py
```

These tests use local TLS/API and UNIX sockets, generated fixture keys and token
replacement. They exercise the real collector's double reads and reject drift,
multiple targets, missing API and concurrent fetch admission. They do not
contact a cluster or change the stream.

`qualify_cpu.py` is an opt-in container comparison. It requires a fixture
certificate/key mounted at `/fixture`, writable owned RAM/results directories,
no external network, non-root execution, pids 48, memory/swap 96Mi and a manifest
whose 100m or 250m CPU limit exactly matches the actual cgroup. It runs the
manifest's API/probe commands and rejects an unlimited or different quota.
Do not run it against a production target or infer an outage rate from timings.
The [dated checkpoint](../../../docs/v3/cra-observability-closure.md) records the
operated comparison separately from this source port.
