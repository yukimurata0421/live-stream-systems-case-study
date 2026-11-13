#!/usr/bin/env python3
"""Keep the pinned maintenance checks running while the operator inspects public players."""
import hashlib
import json
import os
from pathlib import Path
import re
import select
import subprocess
import sys

CORE = Path('/opt/stream-v3-manual-maintenance/current')
# Exact public maintenance sources; rebuild and review these pins when upgrading the core.
PINS = {
    'manual_maintenance.py': 'e1333cd2161eac220d5417e569effebb6cef75fb728430a4e0f30cb384664125',
    'maintenance_host.py': 'af65d55ab88a36475f93c1fec97ca4e013dc3a1b5797b30e82489c29c592f13c',
    'maintenance_state.py': 'aa7e1222767254651a46b46ec8422dc39ccf40c54cda88735f9a5c22a68531e9',
    'stream-v3-maintenance': '2980febf7aa3ddb52d723fb2b55295f48e885fab4eed3acc7c5b3c5de4adbabe',
}


def confirm_while_observing(maintenance, *, stream=None, output=None, wait=None):
    stream = sys.stdin if stream is None else stream
    output = sys.stderr if output is None else output
    wait = select.select if wait is None else wait
    print('Five minutes of stability confirmed. Live checks continue while waiting for input.', file=output, flush=True)
    print('Are both public players showing normal video/audio and updating live data? [yes/no] ',
          file=output, end='', flush=True)
    while True:
        # Exact installed checks: identity, authority, AV/output progression, connection and MAX_GAP.
        maintenance.sleep(maintenance.interval)
        if maintenance.verification_sample() < maintenance.stable_seconds:
            raise ValueError('live_stability_not_yet_confirmed')
        ready, _, _ = wait([stream], [], [], 5)
        if ready:
            return stream.readline().strip().casefold() in ('yes', 'y')


def main():
    try:
        if os.geteuid() != 0 or not sys.stdin.isatty() or not sys.stderr.isatty() or len(sys.argv) != 1:
            raise ValueError('root_terminal_without_confirmation_flags_required')
        for name, expected in PINS.items():
            path = CORE / name
            info = path.stat()
            if info.st_uid != 0 or info.st_mode & 0o022 or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise ValueError('installed_maintenance_core_changed')
        sys.path.insert(0, str(CORE))
        from manual_maintenance import Maintenance, HostBackend, validate_config
        from maintenance_state import Store, guard, read_json
        launch = read_json(Path('/var/lib/stream-v3-nvidia-smoke/launch.json'), uid=0)
        store = Store()
        if guard(store.root, uid=0)['reason'] == 'maintenance_state_invalid':
            raise ValueError('untrusted_session_store')
        config = validate_config(read_json(Path('/etc/stream-v3-manual-maintenance/config.json'), uid=0))
        with store.lock():
            maintenance = Maintenance(store, HostBackend(config), config, interval=2, stable_seconds=300)
            if (maintenance.state['session_id'] != launch['session_id']
                    or maintenance.state['generation'] != launch['generation']
                    or maintenance.state['phase'] not in ('VERIFYING', 'RESTORING')
                    or launch['stage'] != 'ARMED'):
                raise ValueError('not_the_armed_recovery_session')
            print('Checking the same session; observations continue while waiting for input.', file=sys.stderr, flush=True)
            maintenance.off(confirm=lambda: confirm_while_observing(maintenance))
            print(json.dumps(maintenance.status()))
        return 0
    except (OSError, ValueError, KeyError, TypeError, EOFError, KeyboardInterrupt, subprocess.SubprocessError) as exc:
        reason = str(exc) if isinstance(exc, ValueError) and re.fullmatch('[a-z_]{1,100}', str(exc)) else type(exc).__name__
        print(json.dumps({'result': 'HELD', 'reason': reason, 'inhibition_retained': True}))
        return 130 if isinstance(exc, KeyboardInterrupt) else 1


if __name__ == '__main__':
    raise SystemExit(main())
