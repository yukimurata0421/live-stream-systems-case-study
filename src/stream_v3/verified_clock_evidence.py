"""Bounded native clock evidence; separate from normal RAM display snapshots."""
import io
import json
import zipfile

from .viewer_clock import GrayImage
from .viewer_clock_worker import EvidenceStore,json_bytes

SCHEMA='stream_v3.clock_pair_evidence.v1'
MAX_BUNDLE_BYTES=256*1024
ROLES={'previous.pgm':'JST_previous','current.pgm':'JST_current',
       'jst.pgm':'JST_current','utc.pgm':'UTC_current','utc_context.pgm':'UTC_context_native'}
ABNORMAL=('OCR_UNCERTAIN','STALLED_SUSPECT','STALLED_CONFIRMED','BLACK_FRAME')


def roles_for(row):
    if (row.get('clock_evidence_kind')=='roi_retry' and row.get('ocr_expanded_utc_attempted') is True
            and row.get('clock_state') in ('ADVANCING_CONFIRMED','WARMUP') and row.get('clock_pair_consistent') is True):
        return {key:ROLES[key] for key in ('jst.pgm','utc.pgm','utc_context.pgm')}
    if row.get('clock_evidence_kind')=='periodic_roi' and row.get('clock_state')=='ADVANCING_CONFIRMED':
        return {key:ROLES[key] for key in ('jst.pgm','utc.pgm','utc_context.pgm')}
    if row.get('clock_state') in ABNORMAL:return ROLES
    raise ValueError('native_clock_evidence_not_abnormal')


class ClockEvidenceStore(EvidenceStore):
    def save(self,row,images,*,now,clock_images):
        roles=roles_for(row)
        if row.get('capture_ok') is not True:
            raise ValueError('native_clock_evidence_not_abnormal')
        named={name+'.pgm':clock_images[name] for name in ('jst','utc','utc_context')}
        if 'previous.pgm' in roles:named.update({'previous.pgm':images[0],'current.pgm':images[1]})
        if any(image.width>320 or image.height>96 for image in named.values()):
            raise ValueError('native_clock_roi_too_large')
        metadata={**row,'evidence_schema':SCHEMA,'roi_roles':roles}
        bundle=io.BytesIO()
        with zipfile.ZipFile(bundle,'w',compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr('observation.json',json_bytes(metadata))
            for name,image in named.items():archive.writestr(name,image.pgm())
        return self.import_bundle(bundle.getvalue(),row,now=now)

    def import_bundle(self,content,row,*,now):
        try:self.validate_bundle(content,row)
        except (zipfile.BadZipFile,KeyError,TypeError,UnicodeError) as error:
            raise ValueError('native_clock_bundle_invalid') from error
        return self._commit(row['platform'],content,now=now)

    def validate_bundle(self,content,row):
        if len(content)>MAX_BUNDLE_BYTES:raise ValueError('native_clock_bundle_too_large')
        roles=roles_for(row)
        if row.get('platform') not in self.platforms or row.get('capture_ok') is not True:
            raise ValueError('native_clock_evidence_not_abnormal')
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            infos=archive.infolist()
            if len(infos)!=len(roles)+1 or {x.filename for x in infos}!={'observation.json',*roles}:
                raise ValueError('native_clock_members_invalid')
            if sum(x.file_size for x in infos)>MAX_BUNDLE_BYTES or archive.getinfo('observation.json').file_size>16384:
                raise ValueError('native_clock_bundle_too_large')
            metadata=json.loads(archive.read('observation.json'))
            if metadata.get('evidence_schema')!=SCHEMA or metadata.get('roi_roles')!=roles or metadata.get('clock_evidence_kind')!=row.get('clock_evidence_kind'):
                raise ValueError('native_clock_schema_invalid')
            for key in ('platform','sample_id','image_sha256','captured_at_utc','capture_width','capture_height'):
                if metadata.get(key)!=row.get(key) or metadata.get(key) is None:
                    raise ValueError('native_clock_sample_mismatch')
            for name in roles:
                image=GrayImage.from_pgm(archive.read(name))
                if image.width>320 or image.height>96:raise ValueError('native_clock_roi_too_large')
