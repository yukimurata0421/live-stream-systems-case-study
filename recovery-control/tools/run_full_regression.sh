#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "${script_dir}/.." && pwd)"
python="${project_root}/.venv/bin/python"
fixed_runtime="${project_root}/tools/sqlite_runtime/run-fixed.sh"

if [[ ! -x "${python}" ]]; then
  printf 'CRA_FULL_REGRESSION_ENVIRONMENT_FAILURE: missing executable %s\n' "${python}" >&2
  exit 2
fi

export PYTHONPATH="${project_root}/src:${project_root}${PYTHONPATH:+:${PYTHONPATH}}"
cd "${project_root}"

"${fixed_runtime}" "${python}" "${project_root}/tools/sqlite_runtime/check_fixed.py" --project-root "${project_root}"
exec "${fixed_runtime}" "${python}" -m pytest "$@"
