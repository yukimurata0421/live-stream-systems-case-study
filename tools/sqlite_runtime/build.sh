#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "${script_dir}/../.." && pwd)"
python="${PYTHON:-${project_root}/.venv/bin/python}"
runtime_root="${project_root}/.runtime/sqlite-3.51.3"
download_dir="${runtime_root}/downloads"
source_dir="${runtime_root}/source"
build_dir="${runtime_root}/build"
install_dir="${runtime_root}/install"
archive="${download_dir}/sqlite-autoconf-3510300.tar.gz"
source_url="https://www.sqlite.org/2026/sqlite-autoconf-3510300.tar.gz"
archive_sha256="81f5be397049b0cae1b167f2225af7646fc0f82e4a9b3c48c9ea3a533e21d77a"
sqlite3_c_sha3_256="32d5424f97e0a7fc5ed2f6335afbb58be4e0298bd7117a34e39d345ff13d859e"

mkdir -p "${download_dir}" "${source_dir}" "${build_dir}" "${install_dir}"
if [[ ! -f "${archive}" ]]; then
  curl --fail --silent --show-error --location --output "${archive}" "${source_url}"
fi
printf '%s  %s\n' "${archive_sha256}" "${archive}" | sha256sum --check --status

if [[ ! -f "${source_dir}/sqlite3.c" ]]; then
  tar -xzf "${archive}" -C "${source_dir}" --strip-components=1
fi

actual_source_sha3="$(openssl dgst -sha3-256 "${source_dir}/sqlite3.c" | awk '{print $NF}')"
if [[ "${actual_source_sha3}" != "${sqlite3_c_sha3_256}" ]]; then
  printf 'sqlite3.c SHA3-256 mismatch: expected=%s actual=%s\n' "${sqlite3_c_sha3_256}" "${actual_source_sha3}" >&2
  exit 1
fi

if [[ ! -f "${build_dir}/Makefile" ]]; then
  (
    cd "${build_dir}"
    CFLAGS="-O2 -fPIC" "${source_dir}/configure" \
      --prefix="${install_dir}" \
      --disable-static \
      --soname=legacy \
      --all \
      --fts3 \
      --update-limit
  )
fi

make -C "${build_dir}" --jobs="$(getconf _NPROCESSORS_ONLN)"
make -C "${build_dir}" install

LD_LIBRARY_PATH="${install_dir}/lib" \
  "${python}" - <<'PY'
import sqlite3

if sqlite3.sqlite_version != "3.51.3":
    raise SystemExit(f"unexpected SQLite runtime: {sqlite3.sqlite_version}")
print(sqlite3.sqlite_version)
PY
