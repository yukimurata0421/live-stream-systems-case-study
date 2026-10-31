#!/usr/bin/env python3
"""Run in an owned transient service; never operate host delivery services."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent

def put(root, path, text):
    p = root / path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)

def main():
    p = argparse.ArgumentParser()
    p.add_argument('evidence', type=Path)
    p.add_argument('case', choices=['probe', 'success', 'failed_reception', 'job_failure', 'timeout', 'monitor_crash', 'gain_change'])
    p.add_argument('--full', action='store_true')
    args = p.parse_args()
    args.evidence.resolve().mkdir(parents=True, exist_ok=True)
    root = args.evidence.resolve() / args.case
    root.mkdir()  # Never reuse or overwrite prior failures or ledgers.
    for d in ['etc/default', 'etc/systemd/system', 'var/lib/lab', 'var/lib/airspy-reception-guard',
              'evidence', 'control', 'app', 'local/bin', 'opt/airspy-reception-guard/current']:
        (root / d).mkdir(parents=True, exist_ok=True)
    subprocess.run(['cc', '-O1', '-Wall', '-Wextra', '-o', str(root / 'local/bin/airspy_adsb'), str(HERE / 'receiver.c')], check=True)
    for dest in ['app/receiver_guard.py', 'opt/airspy-reception-guard/current/receiver_guard.py']:
        shutil.copyfile(SOURCE / 'receiver_guard.py', root / dest)
    shutil.copyfile(HERE / 'controller.py', root / 'app/controller.py')
    cg = Path('/sys/fs/cgroup' + Path('/proc/self/cgroup').read_text().strip().split('::')[1])
    assert cg.name == 'supervisor' and 'airspy-systemd-lab-' in cg.parent.name, cg
    payload = cg.parent / ('payload-' + args.case)
    payload.mkdir()
    host = dict(namespaces={n: os.readlink('/proc/1/ns/' + n) for n in ['user', 'mnt', 'pid', 'net', 'uts', 'cgroup']})
    put(root, 'control/host.json', json.dumps(host))
    put(root, 'control/mode', args.case)
    put(root, 'control/period', '60' if args.full else '1')
    if args.full:
        put(root, 'control/full', '')
    put(root, 'evidence/dev-console.log', '')
    put(root, 'etc/machine-id', uuid.uuid4().hex + '\n')
    put(root, 'etc/passwd', 'root:x:0:0:root:/root:/bin/sh\nsystemd-journal:x:999:999:journal:/:/usr/sbin/nologin\n')
    put(root, 'etc/group', 'root:x:0:\nsystemd-journal:x:999:\n')
    put(root, 'etc/nsswitch.conf', 'passwd: files\ngroup: files\nhosts: files\n')
    put(root, 'etc/default/airspy_adsb', 'GAIN="16"\nBIAS="-b"\n')
    put(root, 'etc/systemd/journald.conf', '[Journal]\nStorage=volatile\nRuntimeMaxUse=16M\nRateLimitIntervalSec=0\n')
    units = root / 'etc/systemd/system'
    (units / 'lab.target').write_text('[Unit]\nDescription=Isolated Airspy test\nDefaultDependencies=no\nWants=systemd-journald.socket systemd-journald.service lab-controller.service\n')
    (units / 'systemd-journald.socket').write_text('[Unit]\nDefaultDependencies=no\n[Socket]\nListenDatagram=/run/systemd/journal/socket\nListenStream=/run/systemd/journal/stdout\nPassCredentials=yes\nSocketMode=0666\n')
    (units / 'systemd-journald.service').write_text('[Unit]\nDefaultDependencies=no\nRequires=systemd-journald.socket\nAfter=systemd-journald.socket\n[Service]\nType=notify\nExecStart=/usr/lib/systemd/systemd-journald\nStandardOutput=null\n')
    (units / 'lab-controller.service').write_text('[Unit]\nDefaultDependencies=no\nAfter=systemd-journald.service\n[Service]\nType=exec\nExecStart=/usr/bin/python3 /app/controller.py\nStandardOutput=journal\nStandardError=journal\nTimeoutStartSec=1000\n')
    (units / 'readsb.service').write_text('[Unit]\nDefaultDependencies=no\n[Service]\nType=simple\nExecStart=/usr/bin/python3 /app/controller.py readsb\nRestart=no\n')
    (units / 'airspy_adsb.service').write_text('[Unit]\nDefaultDependencies=no\n[Service]\nType=simple\nExecStart=/usr/local/bin/airspy_adsb\nExecStartPost=/usr/bin/sleep 2\nRestart=no\nTimeoutStopSec=30\n')
    guardunit = (SOURCE / 'airspy-reception-guard.service').read_text()
    # Recovery is opt-in in the public unit; enable it only in this isolated manager.
    guardunit = guardunit.replace('receiver_guard.py observe-only\n', 'receiver_guard.py observe\n')
    # Disable default dependencies for the minimal target; retain Service hardening.
    guardunit = guardunit.replace('[Unit]\n', '[Unit]\nDefaultDependencies=no\n')
    (units / 'airspy-reception-guard.service').write_text(guardunit)
    (units / 'lab-diagnostic.service').write_text(guardunit.replace('ExecStart=/usr/bin/python3 /opt/airspy-reception-guard/current/receiver_guard.py observe', 'ExecStart=/usr/bin/python3 /app/controller.py diagnostic'))
    (units / 'airspy-reception-guard.timer').write_text('[Unit]\nDefaultDependencies=no\n[Timer]\nOnActiveSec=1\nOnUnitInactiveSec=' + ('30' if args.full else '1') + '\nAccuracySec=100ms\nUnit=airspy-reception-guard.service\n')
    env = (SOURCE / 'airspy-reception.env.example').read_text()
    if not args.full:
        env = '\n'.join('AIRSPY_RX_' + k + '=' + str(v) for k, v in dict(RATE_RATIO=.10, BASELINE_SEC=30, GRACE_SEC=2, SUSTAIN_SEC=3,
                           STARTUP_WINDOW_SEC=120, SAMPLE_MAX_AGE_SEC=5, READSB_MAX_AGE_SEC=5,
                           MIN_SAMPLES=3, BASELINE_MIN_SAMPLES=3, BASELINE_MIN_POSITIONS=2, COOLDOWN_SEC=1).items()) + '\n'
    put(root, 'etc/airspy-reception-guard.env', env)
    # The bwrap AppArmor profile denies the mount operations systemd requires.
    # Build a minimal root in new user/mount namespaces without changing profiles.
    entry = '''import ctypes,os,sys,time
from pathlib import Path
root=Path(sys.argv[1]); cg=sys.argv[2]; fs=root/'mnt'; fs.mkdir()
libc=ctypes.CDLL(None,use_errno=True)
def mount(src,dst,typ=None,flags=0,data=None):
    def b(x): return str(x).encode() if x is not None else None
    if libc.mount(b(src),b(dst),b(typ),ctypes.c_ulong(flags),b(data)):
        raise OSError(ctypes.get_errno(),f'mount {src} {dst}')
mount(None,'/',flags=16384|262144)
mount('tmpfs',fs,'tmpfs',2|4,'mode=0755,size=32M')
def bind(src,dst,ro=False,file=False):
    p=fs/dst.lstrip('/'); p.parent.mkdir(parents=True,exist_ok=True)
    if file: p.touch()
    else: p.mkdir(exist_ok=True)
    mount(src,p,flags=4096)
    if ro: mount(None,p,flags=4096|32|1|2|4)
bind('/usr','/usr',True)
for s in ['bin','sbin','lib','lib64']: (fs/s).symlink_to('usr/'+s)
for src,dst,ro in [('local','/usr/local',True),('etc','/etc',True),('app','/app',True),('opt','/opt',True),('control','/control',True),('var','/var',False),('evidence','/evidence',False)]:
    bind(root/src,dst,ro)
if (root/'control/mode').read_text()=='gain_change':
    bind(root/'etc/default','/etc/default')  # Synthetic configuration owned by this case only.
for name in ['run','tmp','dev','dev/shm']:
    d=fs/name; d.mkdir(parents=True,exist_ok=True); mount('tmpfs',d,'tmpfs',2|4,'mode=0755,size=32M')
for name in ['null','zero','random','urandom']: bind('/dev/'+name,'/dev/'+name,file=True)
bind(root/'evidence/dev-console.log','/dev/console',file=True)
(fs/'proc').mkdir(); mount('proc',fs/'proc','proc',2|4|8)
(fs/'sys').mkdir(); mount('sysfs',fs/'sys','sysfs',1|2|4|8); bind(cg,'/sys/fs/cgroup')
for name in ['root','home']: (fs/name).mkdir()
os.chdir(fs); Path('.oldroot').mkdir()
if libc.syscall(155,b'.',b'.oldroot'): raise OSError(ctypes.get_errno(),'pivot_root')
os.chdir('/')
if libc.umount2(b'/.oldroot',2): raise OSError(ctypes.get_errno(),'detach old root')
Path('/.oldroot').rmdir()
os.environ.clear(); os.environ.update(PATH='/usr/sbin:/usr/bin:/sbin:/bin',LANG='C.UTF-8',HOME='/root',container='airspy-lab')
Path('/evidence/entry-ready').touch()
while not Path('/evidence/entry-go').exists(): time.sleep(.05)
os.execv('/usr/lib/systemd/systemd',['systemd','--system','--unit=lab.target','--log-target=console','--show-status=no'])
'''
    put(root, 'mount-entry.py', entry)
    cmd = ['/usr/bin/unshare', '--user', '--map-users=0:0:4294967295', '--map-groups=0:0:4294967295',
           '--mount', '--pid', '--fork', '--net', '--ipc', '--uts', '--cgroup',
           '/usr/bin/python3', str(root / 'mount-entry.py'), str(root), str(payload)]
    metadata = dict(case=args.case, full_timing=args.full, command=cmd,
                    receiver_guard_sha256=hashlib.sha256((SOURCE / 'receiver_guard.py').read_bytes()).hexdigest(),
                    controller_sha256=hashlib.sha256((root / 'app/controller.py').read_bytes()).hexdigest(),
                    fixture_sha256=hashlib.sha256((root / 'local/bin/airspy_adsb').read_bytes()).hexdigest(),
                    payload_cgroup=str(payload), host=host)
    (root / 'launch.json').write_text(json.dumps(metadata, indent=2))
    def enter_payload():
        (payload / 'cgroup.procs').write_text(str(os.getpid()))
    with (root / 'console.log').open('w') as output:
        child = subprocess.Popen(cmd, stdout=output, stderr=subprocess.STDOUT, preexec_fn=enter_payload)
        try:
            deadline = time.monotonic() + 10
            while not (root / 'evidence/entry-ready').exists():
                if child.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('namespace entry failed; see console.log')
                time.sleep(.05)
            # Move the outer unshare waiter out of the nested manager subtree.
            (cg / 'cgroup.procs').write_text(str(child.pid))
            (root / 'evidence/entry-go').touch()
            rc = child.wait(timeout=1000 if args.full else 200)
        except BaseException as exc:
            (root / 'launcher-failure.json').write_text(json.dumps(dict(error=repr(exc))))
            # Reap only this case subtree of the dedicated lab unit.
            (payload / 'cgroup.kill').write_text('1')
            child.kill()
            child.wait(timeout=5)
            raise
    metadata['returncode'] = rc
    metadata['cgroup_events'] = (payload / 'cgroup.events').read_text()
    metadata['cgroup_procs'] = (payload / 'cgroup.procs').read_text()
    (root / 'completion.json').write_text(json.dumps(metadata, indent=2))
    result = json.loads((root / 'evidence/result.json').read_text()) if (root / 'evidence/result.json').exists() else None
    print(json.dumps(dict(case=args.case, returncode=rc, result=result)), flush=True)
    # Namespace poweroff may signal the outer unshare with SIGINT; retain raw rc.
    return 0 if rc in (0, -2) and result and result.get('passed') and 'populated 0' in metadata['cgroup_events'] else 1

if __name__ == '__main__':
    sys.exit(main())
