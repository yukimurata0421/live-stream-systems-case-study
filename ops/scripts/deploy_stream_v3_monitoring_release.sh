#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="${STREAM_V3_REPO_ROOT:-$(cd "${script_dir}/../.." && pwd)}"
release_root="${STREAM_V3_RELEASE_ROOT:-/opt/stream-v3-releases}"
current_link="${STREAM_V3_CURRENT_LINK:-/opt/stream_v3}"
state_root="${STREAM_RUNTIME_STATE_DIR:-/var/lib/stream-v3/observability-monitor}"
tag="${1:-}"

if [[ "${STREAM_V3_RELEASE_APPLY:-0}" != "1" ]]; then
  echo "refusing deployment: set STREAM_V3_RELEASE_APPLY=1 during an explicit release window" >&2
  exit 2
fi
if [[ -z "${tag}" ]]; then
  echo "usage: STREAM_V3_RELEASE_APPLY=1 $0 <annotated-tag>" >&2
  exit 2
fi
if [[ -n "$(git -C "${repo_root}" status --porcelain --untracked-files=no)" ]]; then
  echo "refusing deployment: tracked worktree is dirty" >&2
  exit 2
fi
if [[ "$(git -C "${repo_root}" cat-file -t "${tag}" 2>/dev/null || true)" != "tag" ]]; then
  echo "refusing deployment: ${tag} is not an annotated tag" >&2
  exit 2
fi

revision="$(git -C "${repo_root}" rev-list -n 1 "${tag}")"
release_dir="${release_root}/${tag}"
install -d -m 0755 "${release_root}"
install -d -m 0750 "${state_root}"
if [[ -e "${release_dir}" ]]; then
  existing="$(git -C "${release_dir}" rev-parse HEAD 2>/dev/null || true)"
  [[ "${existing}" == "${revision}" ]] || {
    echo "refusing deployment: existing release directory has another revision" >&2
    exit 2
  }
else
  git -C "${repo_root}" worktree add --detach "${release_dir}" "${tag}"
fi

if [[ -e "${current_link}" && ! -L "${current_link}" ]]; then
  echo "refusing deployment: ${current_link} exists and is not a symlink" >&2
  exit 2
fi
temporary_link="${current_link}.${revision}.tmp"
ln -s "${release_dir}" "${temporary_link}"
mv -Tf "${temporary_link}" "${current_link}"

revision_tmp="${state_root}/deployed-revision.env.tmp"
{
  echo "STREAM_V3_DEPLOYED_REVISION=${tag}"
  echo "STREAM_V3_DEPLOYED_COMMIT=${revision}"
} >"${revision_tmp}"
mv -f "${revision_tmp}" "${state_root}/deployed-revision.env"

units=(
  adsb-streamnew-stream1090-report.service
  adsb-streamnew-stream1090-report.timer
  adsb-streamnew-upstream-report.service
  adsb-streamnew-upstream-report.timer
  adsb-streamnew-notify.service
  adsb-streamnew-notify.timer
  adsb-streamnew-prometheus-exporter.service
  stream-v3-observability-monitor.service
  stream-v3-health-snapshot.service
  stream-v3-health-snapshot.timer
  stream-v3-monitoring-watchdog.service
  stream-v3-monitoring-watchdog.timer
  stream-v3-shadow-sli.service
  stream-v3-shadow-sli.timer
  stream-v3-operational-reliability-rollup.service
  stream-v3-operational-reliability-rollup.timer
  stream-v3-reliability-burn-evaluator.service
  stream-v3-reliability-burn-evaluator.timer
  stream-v3-external-blackbox-import.service
  stream-v3-external-blackbox-import.timer
)
for unit in "${units[@]}"; do
  sudo install -m 0644 "${release_dir}/ops/systemd/${unit}" "/etc/systemd/system/${unit}"
done

arena_anchor_dropin_dir="/etc/systemd/system/stream-v3-persistent-anchor-observer.service.d"
sudo install -d -m 0755 "${arena_anchor_dropin_dir}"
sudo install -m 0644 \
  "${release_dir}/ops/systemd/arena-server/stream-v3-persistent-anchor-observer.service.d/10-host-role.conf" \
  "${arena_anchor_dropin_dir}/10-host-role.conf"

sudo systemctl daemon-reload
# The control loop is the single notification writer. Keep the legacy timer
# installed but disabled so a release cannot reintroduce a second writer.
sudo systemctl disable --now adsb-streamnew-notify.timer
sudo systemctl enable --now \
  adsb-streamnew-stream1090-report.timer \
  adsb-streamnew-upstream-report.timer \
  stream-v3-health-snapshot.timer \
  stream-v3-monitoring-watchdog.timer \
  stream-v3-shadow-sli.timer \
  stream-v3-operational-reliability-rollup.timer \
  stream-v3-reliability-burn-evaluator.timer \
  stream-v3-external-blackbox-import.timer
sudo systemctl restart adsb-streamnew-prometheus-exporter.service stream-v3-observability-monitor.service
sudo systemctl start \
  stream-v3-operational-reliability-rollup.service \
  stream-v3-reliability-burn-evaluator.service \
  stream-v3-external-blackbox-import.service

echo "deployed tag=${tag} revision=${revision} release_dir=${release_dir}"
