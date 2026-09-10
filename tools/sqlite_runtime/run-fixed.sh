#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "${script_dir}/../.." && pwd)"
library_dir="${project_root}/.runtime/sqlite-3.51.3/install/lib"

if [[ ! -f "${library_dir}/libsqlite3.so.0" ]]; then
  printf 'fixed SQLite runtime is missing; run tools/sqlite_runtime/build.sh first\n' >&2
  exit 1
fi

export LD_LIBRARY_PATH="${library_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
exec "$@"
