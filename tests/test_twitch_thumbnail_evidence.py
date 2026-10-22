import hashlib
import json
import os
import time
import zipfile

from stream_v3.twitch_thumbnail_evidence import ThumbnailEvidenceStore
from stream_v3.viewer_clock import GrayImage


def observation(value,stream='stream'):
    return {'platform':'twitch_thumbnail','stream_id':stream,'image_sha256':hashlib.sha256(str(value).encode()).hexdigest(),
            'capture_ok':True,'clock_state':'OCR_UNCERTAIN','clock_rois':{'jst':[.019,.968,.04,.022],'utc':[.081,.968,.04,.022]}}


def images():
    return (GrayImage(77,24,bytes([80])*77*24),GrayImage(76,24,bytes([180])*76*24))


def test_changed_anomaly_images_are_saved_once_with_roles_and_survive_restart(tmp_path):
    now=time.time();store=ThumbnailEvidenceStore(tmp_path)
    first=store.save(observation(1),images(),now=now)
    assert store.save(observation(1),images(),now=now+1)==first and store.last_status=='reused'
    second=store.save(observation(2),images(),now=now+2)
    assert second!=first and len(list(tmp_path.glob('*.zip')))==2
    restarted=ThumbnailEvidenceStore(tmp_path)
    assert restarted.save(observation(2),images(),now=now+3)==second and restarted.last_status=='reused'
    with zipfile.ZipFile(tmp_path/second) as archive:
        assert set(archive.namelist())=={'observation.json','jst.pgm','utc.pgm'}
        row=json.loads(archive.read('observation.json'))
        assert row['roi_roles']=={'jst.pgm':'JST_HH_MM_SS','utc.pgm':'UTC_HH_MM_SS'}
    assert not list(tmp_path.glob('*.pgm'))
    other_stream=restarted.save(observation(2,'next-stream'),images(),now=now+4)
    assert other_stream!=second


def test_count_and_allocated_bytes_are_bounded_and_current_evidence_is_preserved(tmp_path):
    now=time.time();store=ThumbnailEvidenceStore(tmp_path,max_bytes=16384,max_bundles=3)
    latest=None
    for i in range(20):
        previous=latest;latest=store.save(observation(i),images(),now=now+i)
        assert latest and (tmp_path/latest).exists()
        if previous:assert (tmp_path/previous).exists()
        assert len(list(tmp_path.glob('*.zip')))<=3
        assert sum(store.disk_bytes(p) for p in tmp_path.glob('*.zip'))<=16384
    assert len(store.samples)<=3


def test_full_capacity_does_not_delete_current_minimum_evidence(tmp_path):
    now=time.time();store=ThumbnailEvidenceStore(tmp_path,max_bytes=4096,max_bundles=2)
    first=store.save(observation(1),images(),now=now);assert first
    assert store.save(observation(2),images(),now=now+1) is None
    assert store.last_status=='capacity_limited' and (tmp_path/first).exists()
    assert sum(store.disk_bytes(p) for p in tmp_path.glob('*.zip'))<=4096


def test_retention_is_enforced_and_pruned_sample_can_be_replaced(tmp_path):
    now=time.time();store=ThumbnailEvidenceStore(tmp_path,retention_seconds=10)
    old=store.save(observation(1),images(),now=now)
    os.utime(tmp_path/old,(now-11,now-11))
    new=store.save(observation(1),images(),now=now+1)
    assert new!=old and not (tmp_path/old).exists() and (tmp_path/new).exists()


def test_failed_image_deduplication_includes_roi_configuration(tmp_path):
    now=time.time();store=ThumbnailEvidenceStore(tmp_path);row=observation(1)
    first=store.save(row,images(),now=now)
    row['clock_rois']['utc']=[.0795,.968,.0415,.022]
    assert store.save(row,images(),now=now+1)!=first


def test_normal_frames_failed_captures_and_full_frames_are_never_saved(tmp_path):
    store=ThumbnailEvidenceStore(tmp_path);now=time.time();row=observation(1)
    row['clock_state']='ADVANCING_CONFIRMED'
    assert store.save(row,images(),now=now) is None and store.last_status=='not_required'
    row.update(clock_state='OCR_UNCERTAIN',capture_ok=False)
    assert store.save(row,images(),now=now) is None and store.last_status=='roi_unavailable'
    row['capture_ok']=True;full=GrayImage(1920,1080,bytes(1920*1080))
    assert store.save(row,(full,full),now=now) is None and store.last_status=='roi_unavailable'
    assert not list(tmp_path.glob('*.zip'))
