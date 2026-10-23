import json
import time
import zipfile

import pytest

from stream_v3.verified_clock_evidence import ClockEvidenceStore
from stream_v3.viewer_clock import GrayImage


def sample():
    row={'platform':'oci_relay','clock_state':'OCR_UNCERTAIN','capture_ok':True,
         'sample_id':'a'*24,'image_sha256':'b'*64,'captured_at_utc':'2026-10-23T00:00:00Z',
         'capture_width':1920,'capture_height':1080}
    image=GrayImage(76,24,b'\x80'*(76*24))
    return row,image


def test_native_archive_is_bound_to_sample_and_bounds(tmp_path):
    store=ClockEvidenceStore(tmp_path/'producer',platforms=('oci_relay',),max_bytes=8192)
    row,image=sample();images={'jst':image,'utc':image,'utc_context':image}
    name=store.save(row,(image,image),now=time.time(),clock_images=images)
    content=(store.root/name).read_bytes()
    other=ClockEvidenceStore(tmp_path/'consumer',platforms=('oci_relay',),max_bytes=8192)
    assert other.import_bundle(content,row,now=time.time())
    with pytest.raises(ValueError,match='sample_mismatch'):
        other.import_bundle(content,{**row,'sample_id':'c'*24},now=time.time())
    with pytest.raises(ValueError,match='bundle_invalid'):other.import_bundle(b'bad zip',row,now=time.time())
    with zipfile.ZipFile(store.root/name) as archive:
        assert len(archive.namelist())==6
        assert json.loads(archive.read('observation.json'))['roi_roles']['utc.pgm']=='UTC_current'
    assert len(list(other.root.glob('*.zip')))==1


def test_unsampled_normal_images_are_not_persisted_and_capacity_is_not_extended(tmp_path):
    row,image=sample();images={'jst':image,'utc':image,'utc_context':image}
    store=ClockEvidenceStore(tmp_path,platforms=('oci_relay',),max_bytes=4096)
    with pytest.raises(ValueError,match='not_abnormal'):
        store.save({**row,'clock_state':'ADVANCING_CONFIRMED'},(image,image),now=time.time(),clock_images=images)
    assert not list(tmp_path.glob('*.zip'))
    first=store.save(row,(image,image),now=time.time(),clock_images=images)
    assert first is not None
    assert store.save(row,(image,image),now=time.time(),clock_images=images) is None
    assert list(tmp_path.glob('*.zip'))==[tmp_path/first]


def test_opt_in_diagnostic_sample_saves_only_native_clock_crops(tmp_path):
    row,image=sample();row.update(clock_state='ADVANCING_CONFIRMED',clock_evidence_kind='periodic_roi')
    store=ClockEvidenceStore(tmp_path,platforms=('oci_relay',))
    name=store.save(row,None,now=time.time(),clock_images={'jst':image,'utc':image,'utc_context':image})
    with zipfile.ZipFile(store.root/name) as archive:
        assert set(archive.namelist())=={'observation.json','jst.pgm','utc.pgm','utc_context.pgm'}


def test_rescued_frame_is_retained_without_marking_it_abnormal(tmp_path):
    row,image=sample();row.update(clock_state='ADVANCING_CONFIRMED',clock_evidence_kind='roi_retry',ocr_expanded_utc_attempted=True,clock_pair_consistent=True)
    store=ClockEvidenceStore(tmp_path,platforms=('oci_relay',));name=store.save(row,None,now=time.time(),clock_images={'jst':image,'utc':image,'utc_context':image})
    with zipfile.ZipFile(store.root/name) as archive:
        metadata=json.loads(archive.read('observation.json'))
        assert metadata['clock_state']=='ADVANCING_CONFIRMED' and metadata['clock_evidence_kind']=='roi_retry'
