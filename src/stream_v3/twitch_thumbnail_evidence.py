"""Bounded, deduplicated JST/UTC evidence for abnormal provider thumbnails."""
import hashlib
import io
import json
import re
import time
import zipfile

from .viewer_clock_worker import EvidenceStore,json_bytes


class ThumbnailEvidenceStore(EvidenceStore):
    def __init__(self,root,*,max_bytes=8*1024*1024,retention_seconds=7*86400,max_bundles=64):
        if max_bundles<2:raise ValueError('evidence_count_limit_too_small')
        self.max_bundles=max_bundles;self.samples={};self.last_status='not_required'
        super().__init__(root,max_bytes=max_bytes,retention_seconds=retention_seconds,platforms=('twitch_thumbnail',))
        self.prune(now=time.time())
        for path in sorted(self.root.glob('*.zip'),key=lambda p:p.stat().st_mtime):
            try:
                with zipfile.ZipFile(path) as archive:
                    info=archive.getinfo('observation.json')
                    if info.file_size>65536:continue
                    row=json.loads(archive.read(info))
                key=self.sample_key(row)
                if key:self.samples[key]=path
            except (OSError,ValueError,KeyError,zipfile.BadZipFile):continue

    @staticmethod
    def sample_key(row):
        digest=row.get('image_sha256');stream=row.get('stream_id')
        if not isinstance(digest,str) or re.fullmatch('[0-9a-f]{64}',digest) is None or not isinstance(stream,str):return None
        return hashlib.sha256(json_bytes({'image':digest,'stream':stream,'clock_rois':row.get('clock_rois')})).hexdigest()

    def prune(self,*,now,incoming_bytes=0):
        fits=super().prune(now=now,incoming_bytes=incoming_bytes)
        files=sorted(self.root.glob('*.zip'),key=lambda p:p.stat().st_mtime)
        limit=self.max_bundles-int(incoming_bytes>0)
        for path in list(files):
            if len(files)<=limit:break
            if path not in self.active.values():path.unlink();files.remove(path)
        self.samples={key:path for key,path in self.samples.items() if path.exists()}
        return fits and len(files)<=limit

    def save(self,row,images,*,now):
        if row.get('platform')!='twitch_thumbnail':raise ValueError('invalid_evidence_target')
        if row.get('clock_state')!='OCR_UNCERTAIN' and row.get('freshness_state') not in ('STALE_SUSPECT','STALE_CORRELATED'):
            self.last_status='not_required';return None
        if row.get('capture_ok') is not True or len(images)!=2 or any(image.width>320 or image.height>96 for image in images):
            self.last_status='roi_unavailable';return None
        self.prune(now=now)
        key=self.sample_key(row)
        if key is None:self.last_status='roi_unavailable';return None
        existing=self.samples.get(key)
        if existing is not None and existing.exists():
            self.active['twitch_thumbnail']=existing;self.last_status='reused';return existing.name
        bundle=io.BytesIO()
        metadata={**row,'evidence_sample_id':key,'roi_roles':{'jst.pgm':'JST_HH_MM_SS','utc.pgm':'UTC_HH_MM_SS'}}
        with zipfile.ZipFile(bundle,'w',compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr('observation.json',json_bytes(metadata))
            archive.writestr('jst.pgm',images[0].pgm());archive.writestr('utc.pgm',images[1].pgm())
        name=self._commit('twitch_thumbnail',bundle.getvalue(),now=now)
        if name:self.samples[key]=self.root/name
        self.last_status='saved' if name else 'capacity_limited'
        return name
