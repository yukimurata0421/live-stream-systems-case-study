#!/usr/bin/env bash
set -euo pipefail

# This entrypoint builds a committed artifact only. Activation is a separate,
# pinned operation; installing units or restarting services is never implicit.
if [[ "$#" -ne 4 ]]; then
  echo "usage: $0 <repo> <full-commit-sha> <profile-path> <new-output-directory>" >&2
  exit 2
fi
script_dir="$(cd -- "$(dirname -- "$0")" && pwd)"
exec python3 -B "${script_dir}/stream_v3_release.py" build \
  --repo "$1" --commit "$2" --profile "$3" --output "$4"
