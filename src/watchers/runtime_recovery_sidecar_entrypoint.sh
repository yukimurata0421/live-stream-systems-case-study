#!/bin/sh
set -eu

(
    ledger=${FR_EFFECT_LEDGER_FILE:-/state/runtime/fast_recovery_effects.sqlite3}
    while [ ! -f "$ledger" ]; do
        sleep 1
    done
    sleep 1
    exec python3 -m watchers.runtime_recovery_escalation
) &

exec python3 -m stream_v3.control_loop --mode streaming
