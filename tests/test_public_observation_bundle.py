import importlib.util
import sys
from pathlib import Path
import pytest

ROOT=Path(__file__).resolve().parents[1]
SCRIPTS=ROOT/'ops/public-publisher/site/scripts'
sys.path.insert(0,str(SCRIPTS))
import collect_observation_bundle as consumer
NONCE='a'*32;MACHINE='b'*64


def packet(age=20,boot='boot',nonce=NONCE):
    return {'schema':consumer.SCHEMA,'clock':{'schema':'stream.clock_exchange.v1','nonce':nonce,
        'machine_id_sha256':MACHINE,'boot_id':'boot','received':{'mono':1000,'wall':1800000000},
        'sent':{'mono':1000.01,'wall':1800000000.01}},
        'artifacts':{'stream-v3-prometheus.json':{'boot_id':boot,'generated_mono':1000-age,
            'value':{'schema':'stream-v3-prometheus-public.v1','generated_at':1800000000-age}}}}


def accept(v,wall=1900000000):
    return consumer.accept(v,nonce=NONCE,sent={'mono':20,'wall':wall},
        received={'mono':20.1,'wall':wall+.1},machine=MACHINE)


def test_cross_host_wall_offset_does_not_reject_fresh_bundle():
    values,errors=accept(packet())
    assert not errors
    assert values['stream-v3-prometheus.json']['generated_at']==pytest.approx(1900000000.1-20.11)


@pytest.mark.parametrize('kwargs',[{'age':181},{'age':-20},{'boot':'previous'},{'nonce':'c'*32}])
def test_old_rebooted_or_replayed_evidence_is_not_published(kwargs):
    values,errors=accept(packet(**kwargs))
    assert not values and errors


def test_repeated_read_never_refreshes_original_source_age():
    first,_=accept(packet(20));second,_=accept(packet(80),wall=1900000060)
    assert first['stream-v3-prometheus.json']['generated_at']==second['stream-v3-prometheus.json']['generated_at']


def test_unowned_output_name_and_internal_details_are_not_selectable():
    v=packet();v['artifacts']['../internal.json']=v['artifacts'].pop('stream-v3-prometheus.json')
    with pytest.raises(ValueError):accept(v)
    v=packet();v['artifacts']['stream-v3-prometheus.json']['secret']='forbidden'
    assert not accept(v)[0]


def test_partial_bundle_never_refreshes_the_missing_artifact():
    v=packet();v['artifacts']={}
    assert accept(v)==({},{})
