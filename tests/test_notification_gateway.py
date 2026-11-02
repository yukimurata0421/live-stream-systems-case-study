import importlib.util
from pathlib import Path
import pytest

spec=importlib.util.spec_from_file_location('gateway',Path(__file__).resolve().parents[1]/'ops/scripts/stream_v3_notification_gateway.py');gateway=importlib.util.module_from_spec(spec);spec.loader.exec_module(gateway)
VALUE={'route':'discord','content':'fixture','username':'fixture'}


def test_success_is_provider_ack_and_duplicate_is_not_resent(tmp_path):
    sends=[];g=gateway.Gateway(tmp_path/'db',lambda *a:(sends.append(a)or('DELIVERED',204)),clock=lambda:('boot',10))
    assert g.deliver(VALUE)['delivered']
    assert g.deliver(VALUE)['delivered']
    assert len(sends)==1
    g.db.close()
    g=gateway.Gateway(tmp_path/'db',lambda *_:pytest.fail('duplicate'),clock=lambda:('newboot',1))
    assert g.deliver(VALUE)['delivered']


def test_unknown_and_crash_after_dispatch_are_not_retried(tmp_path):
    g=gateway.Gateway(tmp_path/'db',lambda *_:('UNKNOWN',0))
    assert g.deliver(VALUE)['status']=='unknown'
    g.send=lambda *_:pytest.fail('unknown replayed')
    assert g.deliver(VALUE)['status']=='unknown'
    with g.db:g.db.execute("UPDATE receipts SET status='SENDING'")
    assert g.deliver(VALUE)['status']=='unknown'


def test_explicit_provider_failure_can_retry_and_routes_remain_distinct(tmp_path):
    g=gateway.Gateway(tmp_path/'db',lambda *_:('RETRY',503))
    assert not g.deliver(VALUE)['delivered']
    g.send=lambda *_:('DELIVERED',200)
    assert g.deliver(VALUE)['delivered']
    g.send=lambda *_:('RETRY',503)
    assert not g.deliver({**VALUE,'route':'slack'})['delivered']


def test_bad_routes_and_urls_are_rejected_without_egress(tmp_path):
    g=gateway.Gateway(tmp_path/'db',lambda *_:pytest.fail('invalid egress'))
    with pytest.raises(ValueError):g.deliver({**VALUE,'route':'arbitrary'})
    with pytest.raises(ValueError):g.deliver({**VALUE,'url':'http://example.invalid'})
    assert gateway.provider_send('discord','x','x',{'discord':'http://example.invalid'})==('REJECTED',0)


def test_repeat_window_uses_local_monotonic_time(tmp_path):
    now=[0];sends=[]
    g=gateway.Gateway(tmp_path/'db',lambda *a:(sends.append(a)or('DELIVERED',204)),clock=lambda:('boot',now[0]))
    assert g.deliver(VALUE)['delivered'];now[0]=179;g.deliver(VALUE);assert len(sends)==1
    now[0]=180;g.deliver(VALUE);assert len(sends)==2


def test_independent_health_uses_boot_bound_time_and_keeps_unknown_outcomes_visible():
    spec=importlib.util.spec_from_file_location('health_wrapper',Path(__file__).resolve().parents[1]/'ops/scripts/stream_v3_notification_gateway_health.py');health=importlib.util.module_from_spec(spec);spec.loader.exec_module(health)
    value={'schema':'stream.notification_gateway.v1','boot_id':'boot','mono':100,'receipts':{'UNKNOWN':1}}
    conditions=health.conditions(value,boot='boot',mono=110)
    assert not conditions['NotificationGatewayUnavailable'][0]
    assert conditions['NotificationGatewayUnknownOutcome'][0]
    state={}
    for mono,wall in [(0,1800000000),(60,1900000000),(120,1700000000),(180,1800000180)]:
        alerts,state=health.reduce_own({'NotificationGatewayUnavailable':(True,180,'critical')},state,boot='boot',mono=mono,wall=wall)
        assert bool(alerts)==(mono==180)
    assert health.conditions(value,boot='newboot',mono=110)['NotificationGatewayUnavailable'][0]
