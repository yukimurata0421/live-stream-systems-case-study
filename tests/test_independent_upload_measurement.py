"""Source sampling, authenticated receipts and the real upload-recovery reader."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

import pytest

from stream_core.common import upload_samples as records
from stream_core.notifications import upload_source

SCRIPT = Path(__file__).resolve().parents[1] / 'ops/scripts/stream_v3_upload_measurement.py'
spec = importlib.util.spec_from_file_location('independent_upload_sampler', SCRIPT)
sampler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sampler)
WALL = 1_800_000_000.
SOURCE = {'machine_id_sha256': 'a' * 64, 'boot_id': 'source-boot'}
RECEIVER = {'machine_id_sha256': 'b' * 64, 'boot_id': 'receiver-boot'}
NONCE = 'c' * 32


def source_clock(t, offset=0):
    return {'wall': WALL + t + offset, 'mono': 1000. + t}


def receiver_clock(t, offset=0):
    return {'wall': WALL + t + offset, 'mono': 500. + t}


def counter(t, *, offset=0, pid=730, generation='generation-1', connection='socket-1', delta=500000):
    return {'schema_version': 'runtime.ffmpeg_observation.v1', 'observed_at': records.utc(WALL+t+offset-.1),
        'producer_instance_id': 'producer-1', 'sequence': int(t)+1, 'ffmpeg_generation': generation,
        'local_ffmpeg_pid': pid, 'ffmpeg_running': True, 'managed_child_cardinality': '1',
        'protocol_ffmpeg_pid': 0, 'runtime_identity': None, 'target_snapshot_status': 'UNKNOWN',
        'tcp_metrics': {'bytes_sent': 1000000+int(t)*delta, 'bytes_acked': 1000000+int(t)*delta,
                        'socket_cookie': connection}}


def measured(t=60, *, offset=0):
    state = sampler.Sampler({}, identity=SOURCE, host='encoder-host')
    assert state.step(counter(t-60, offset=offset), source_clock(t-60, offset)) is None
    return state.step(counter(t, offset=offset), source_clock(t, offset))


def packet(row, *, at=None, offset=0, nonce=NONCE):
    t = row['sequence_end']-1 if at is None else at
    before, after = source_clock(t, offset), source_clock(t+.002, offset)
    return {'schema': 'stream.upload_measurement_batch.v1', 'status': {**SOURCE,
        'source_host': 'encoder-host', 'source_mono': before['mono'],
        'measurement_epoch': row['measurement_epoch'], 'status': 'READY'},
        'samples': [row], 'clock': {'schema': records.clock.SCHEMA, 'nonce': nonce, **SOURCE,
                                  'received': before, 'sent': after}}


def select(root):
    folder = root / 'upload_measurement';folder.mkdir()
    records.write_json(folder / 'source.json', {'schema': records.MANIFEST_SCHEMA,
        'source_machine': SOURCE['machine_id_sha256'], 'source_host': 'encoder-host',
        'cutover_at_utc': records.utc(WALL-1)})
    return folder


def accept(folder, row, *, at=None, offset=0, nonce=NONCE):
    t = row['sequence_end']-1 if at is None else at
    return sampler.accept_packet(folder, packet(row, at=t, offset=offset, nonce=nonce), nonce,
        receiver_clock(t), receiver_clock(t+.04), expected_machine=SOURCE['machine_id_sha256'],
        host='encoder-host', identity=RECEIVER)


def test_control_identity_is_not_needed_for_valid_physical_measurements():
    row = measured()
    assert row['mbps'] == 4.0 and row['bytes_sent_delta'] == 30_000_000
    assert row['ffmpeg_pid'] == 730
    assert not {'runtime_identity','protocol_ffmpeg_pid','target_snapshot_status','effect_socket'} & row.keys()


@pytest.mark.parametrize('change', ['generation', 'connection', 'pid', 'counter', 'boot'])
def test_rate_never_spans_an_identity_change_or_counter_rollback(change):
    worker = sampler.Sampler({}, identity=SOURCE, host='encoder-host')
    worker.step(counter(0), source_clock(0));worker.step(counter(30), source_clock(30))
    value = counter(60)
    if change == 'generation':value['ffmpeg_generation'] = 'other-generation'
    if change == 'connection':value['tcp_metrics']['socket_cookie'] = 'other-socket'
    if change == 'pid':value['local_ffmpeg_pid'] = 731
    if change == 'counter':value['tcp_metrics']['bytes_sent'] = 1
    if change == 'boot':worker = sampler.Sampler(worker.state, identity={**SOURCE,'boot_id':'new'}, host='encoder-host')
    assert worker.step(value, source_clock(60)) is None


@pytest.mark.parametrize('step', [-3600, -.2, .2, 3600])
def test_source_utc_step_breaks_the_rate_window(step):
    worker = sampler.Sampler({}, identity=SOURCE, host='encoder-host')
    worker.step(counter(0), source_clock(0))
    assert worker.step(counter(60,offset=step), source_clock(60,step)) is None
    assert worker.step(counter(120,offset=step), source_clock(120,step))['mbps'] == 4.0


def test_duplicate_counter_snapshot_cannot_generate_measurements():
    worker = sampler.Sampler({}, identity=SOURCE, host='encoder-host');value = counter(0)
    worker.step(value, source_clock(0))
    assert worker.step(value, source_clock(1)) is None
    with pytest.raises(ValueError):worker.step(value, source_clock(61))


@pytest.mark.parametrize('field,value', [('ffmpeg_running',False),('local_ffmpeg_pid',0),('managed_child_cardinality','2'),('sequence',True)])
def test_missing_or_ambiguous_source_does_not_become_a_zero_rate(field,value):
    raw = counter(60);raw[field] = value
    with pytest.raises(ValueError):sampler.counter_snapshot(raw, source_clock(60))


@pytest.mark.parametrize('bad', [True,-1,float('nan'),float('inf')])
def test_invalid_counter_values_are_rejected(bad):
    raw = counter(60);raw['tcp_metrics']['bytes_sent'] = bad
    with pytest.raises(ValueError):sampler.counter_snapshot(raw, source_clock(60))


def test_valid_zero_progress_is_a_measurement_without_asserting_delivery():
    worker = sampler.Sampler({}, identity=SOURCE, host='encoder-host')
    worker.step(counter(0,delta=0),source_clock(0))
    row = worker.step(counter(60,delta=0),source_clock(60))
    assert row['mbps'] == 0 and 'delivery_verified' not in row


@pytest.mark.parametrize('offset', [-7200,0,7200])
def test_foreign_utc_offset_and_receiver_steps_do_not_change_causal_freshness(tmp_path,offset):
    folder = select(tmp_path);row = measured(offset=offset)
    accept(folder,row,offset=offset)
    for receiver_step in (-10000,0,10000):
        with patch.object(records.clock,'identity',return_value=RECEIVER),patch.object(records.clock,'capture',return_value=receiver_clock(90,receiver_step)):
            latest = records.latest(tmp_path,WALL+90+receiver_step)
            assert latest['mbps'] == 4.0 and 30 <= latest['_source_age'] < 31
            assert latest['source_ts_utc'] == row['source_ts_utc']


def test_repeated_receipt_does_not_renew_or_duplicate_a_cached_sample(tmp_path):
    folder = select(tmp_path);row = measured();assert accept(folder,row) == 1
    before = (folder/'samples.jsonl').read_bytes()
    assert accept(folder,row,at=240) == 0 and (folder/'samples.jsonl').read_bytes() == before
    with patch.object(records.clock,'identity',return_value=RECEIVER),patch.object(records.clock,'capture',return_value=receiver_clock(241)):
        assert records.latest(tmp_path,WALL+241)['_source_age'] > 180


@pytest.mark.parametrize('change', ['nonce','machine','boot','future','delay','rate'])
def test_invalid_packet_is_rejected_before_any_journal_write(tmp_path,change):
    folder = select(tmp_path);row = measured();value = packet(row)
    if change=='nonce':value['clock']['nonce']='d'*32
    if change=='machine':value['clock']['machine_id_sha256']='d'*64
    if change=='boot':value['clock']['boot_id']='other-boot'
    if change=='future':value['samples'][0]['source_mono'] += 50
    if change=='delay':value['clock']['sent']['mono'] += 20
    if change=='rate':value['samples'][0]['mbps'] = 100
    with pytest.raises(ValueError):
        sampler.accept_packet(folder,value,NONCE,receiver_clock(60),receiver_clock(60.04),
            expected_machine=SOURCE['machine_id_sha256'],host='encoder-host',identity=RECEIVER)
    assert not (folder/'samples.jsonl').exists()


def test_receiver_reboot_and_source_outage_do_not_prove_recovery(tmp_path):
    folder=select(tmp_path);accept(folder,measured())
    for identity,now in [({**RECEIVER,'boot_id':'new-boot'},receiver_clock(90)),(RECEIVER,receiver_clock(250))]:
        with patch.object(records.clock,'identity',return_value=identity),patch.object(records.clock,'capture',return_value=now):
            health = upload_source.raw_health(tmp_path/'logs/fast_recovery_events.jsonl',int(now['wall']))
            assert not health['fresh'] and not health['sustained']


def test_fifteen_minute_raw_window_requires_distinct_verified_measurements(tmp_path):
    folder=select(tmp_path)
    for t in range(60,1021,60):accept(folder,measured(t))
    with patch.object(records.clock,'identity',return_value=RECEIVER),patch.object(records.clock,'capture',return_value=receiver_clock(1020.1)):
        health=upload_source.raw_health(tmp_path/'logs/fast_recovery_events.jsonl',int(WALL+1020.1))
        assert health['available'] and health['fresh'] and health['sustained']


def test_missing_new_source_does_not_fall_back_to_a_fresh_legacy_log(tmp_path):
    select(tmp_path);(tmp_path/'logs').mkdir()
    legacy=tmp_path/'logs/fast_recovery_events.jsonl'
    legacy.write_text(json.dumps({'kind':'tcp_send_sample','ts_utc':records.utc(WALL+60),'mbps':4.})+'\n')
    with patch.object(records.clock,'identity',return_value=RECEIVER),patch.object(records.clock,'capture',return_value=receiver_clock(60)):
        assert not upload_source.raw_health(legacy,int(WALL+60))['available']


def test_rotation_and_overlap_keep_one_original_observation_per_sample(tmp_path):
    folder=select(tmp_path);row=measured();accept(folder,row)
    path=folder/'samples.jsonl';path.rename(folder/'samples.jsonl.1');path.write_bytes((folder/'samples.jsonl.1').read_bytes())
    assert len(records.journal_rows(folder))==1
    path.unlink();path.symlink_to(folder/'samples.jsonl.1')
    with pytest.raises(OSError):records.journal_rows(folder)


def test_historical_measurements_are_preserved_and_postcutover_legacy_rows_do_not_vote(tmp_path):
    folder=select(tmp_path);accept(folder,measured())
    old={'kind':'tcp_send_sample','ts_utc':records.utc(WALL-100),'mbps':5.1}
    retired={'kind':'tcp_send_sample','ts_utc':records.utc(WALL+50),'mbps':8.}
    decision={'kind':'typed_policy_decision','ts_utc':records.utc(WALL+60),'intent_type':'NO_ACTION'}
    rows=list(records.merge_events(tmp_path,[old,retired,decision]))
    assert old in rows and decision in rows and retired not in rows
    assert [r['mbps'] for r in rows if r['kind']=='tcp_send_sample']==[5.1,4.]


def test_serve_cli_is_read_only_and_rejects_a_replayed_nonce(tmp_path):
    identity=records.clock.identity();now=records.clock.capture();row=measured()
    row.update(source_machine=identity['machine_id_sha256'],source_boot=identity['boot_id'],source_mono=now['mono']-1)
    records.append_sample(tmp_path/'samples.jsonl',row)
    records.write_json(tmp_path/'status.json',{**identity,'source_mono':now['mono'],'source_host':'encoder-host'})
    before={p.name:p.read_bytes() for p in tmp_path.iterdir()}
    result=subprocess.run([sys.executable,'-I','-B',str(SCRIPT),'serve','--directory',str(tmp_path),'--nonce',NONCE],capture_output=True,text=True)
    assert result.returncode==0
    value=json.loads(result.stdout);assert value['clock']['nonce']==NONCE and len(value['samples'])==1
    assert {p.name:p.read_bytes() for p in tmp_path.iterdir()}==before
    value['clock']['nonce']='d'*32
    with pytest.raises(ValueError):
        records.clock.monotonic_source_age(value['clock'],NONCE,now,records.clock.capture(),machine=identity['machine_id_sha256'],boot_id=identity['boot_id'],source_mono=row['source_mono'])


def test_actual_exporter_hook_preserves_history_and_uses_verified_current_age(tmp_path):
    path = SCRIPT.with_name('stream_v3_prometheus_exporter.py')
    spec = importlib.util.spec_from_file_location('upload_exporter_contract', path)
    exporter = importlib.util.module_from_spec(spec);spec.loader.exec_module(exporter)
    hook_spec = importlib.util.spec_from_file_location('upload_exporter_hook', SCRIPT.with_name('upload_measurement_exporter.py'))
    hook = importlib.util.module_from_spec(hook_spec);hook_spec.loader.exec_module(hook)
    folder = select(tmp_path);accept(folder,measured())
    (tmp_path/'logs').mkdir()
    (tmp_path/'logs/fast_recovery_events.jsonl').write_text(json.dumps({'kind':'tcp_send_sample','ts_utc':records.utc(WALL-100),'mbps':5.1})+'\n')
    # The runtime baseline does not already contain the new reader. Stub its two
    # legacy readers while retaining the real metric-age and freshness functions.
    exporter.tcp_send_rows = lambda root,now: [{'kind':'tcp_send_sample','ts_utc':records.utc(WALL-100),'mbps':5.1}]
    exporter.latest_tcp_send_observation = lambda root,now: {'_ts':WALL+60,'_mbps':9.}
    hook.install(exporter)
    clock = hook.sys.modules['independent_upload_records'].clock
    with patch.object(clock,'identity',return_value=RECEIVER),patch.object(clock,'capture',return_value=receiver_clock(90)):
        assert [r['_mbps'] for r in exporter.tcp_send_rows(tmp_path,now=WALL+90)] == [5.1,4.]
        assert exporter.latest_tcp_send_observation(tmp_path,now=WALL+90)['_source_age'] > 30
        assert exporter.latest_tcp_send_sample(tmp_path,now=WALL+90)['_mbps'] == 4.
    with patch.object(clock,'identity',return_value=RECEIVER),patch.object(clock,'capture',return_value=receiver_clock(400)):
        assert exporter.latest_tcp_send_sample(tmp_path,now=WALL+400) == {}


def test_objective_sli_keeps_recovery_events_separate_from_upload_measurements(tmp_path):
    from stream_core.cli_support.objective_sli import ObjectiveSliContext, objective_sli_payload
    folder = select(tmp_path);accept(folder,measured())
    logs = tmp_path/'logs';logs.mkdir()
    recovery = logs/'fast_recovery_events.jsonl'
    recovery.write_text(json.dumps({'kind':'typed_policy_decision','ts_utc':records.utc(WALL+60),'intent_type':'NO_ACTION'})+'\n')
    context = ObjectiveSliContext(log_base_dir=logs,youtube_watchdog_events_file=logs/'youtube.jsonl',
        fast_recovery_events_file=recovery,stream_engine_events_file=logs/'engine.jsonl',
        stream1090_report_events_file=logs/'overlay.jsonl',upstream_report_events_file=logs/'upstream.jsonl',
        notify_events_file=logs/'notify.jsonl',memory_status_events_file=logs/'memory.jsonl',
        objective_sli_file=tmp_path/'objective.json',objective_sli_events_file=logs/'objective.jsonl')
    before = recovery.read_bytes()
    payload = objective_sli_payload(context,now_ts=int(WALL+60))
    upload = payload['metrics']['upload_budget']['since_samples_started']
    assert upload['sample_count'] == 1 and upload['max_mbps'] == 4.
    assert recovery.read_bytes() == before
