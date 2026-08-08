# Host Maintenance Helpers

These helpers cover the host-level boundary that cannot be represented by the
in-Pod runtime alone. Public CI tests their pure decision logic but does not
install, enable, or start any unit.

## Helpers

- `bin/nvidia_driver_update_check.py` performs a read-only comparison of the
  installed NVIDIA packages, APT candidates, loaded module, runtime-reported
  driver, DKMS state, holds, and APT metadata freshness. It never upgrades a
  package.
- `bin/stream_v3_gpu_startup_gate.py` keeps a newly created streaming Pod behind
  the `stream-v3.io/gpu-ready` scheduling gate until the host GPU, node, and
  device plugin are ready. It also removes stale previous-boot Pods and limits
  recovery RBAC while the GPU is unavailable.
- `bin/stream_v3_postboot_verify.py` correlates the current host boot ID, Pod
  boot annotations, container state, runtime readiness, `h264_nvenc`, and the
  RTMP/RTMPS socket after a reboot.

## Explicit Installation Boundary

A local operator may install the scripts as:

```text
bin/nvidia_driver_update_check.py -> /usr/local/libexec/stream-v3-nvidia-driver-check
bin/stream_v3_gpu_startup_gate.py -> /usr/local/libexec/stream-v3-gpu-startup-gate
bin/stream_v3_postboot_verify.py -> /usr/local/libexec/stream-v3-postboot-verify
```

The matching files under `systemd/` and `apt/` are examples for an explicit
host operation. Copying them, reloading systemd, enabling timers, or changing
APT policy is deliberately outside repository tests and public CI.

The NVIDIA timer observes package drift only. Driver upgrades and reboots
remain manual, separately approved operations. This avoids turning a routine
read-only check into an unplanned interruption of the live encoder.
