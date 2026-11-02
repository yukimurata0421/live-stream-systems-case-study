"""Warning episodes use local elapsed time, independent of source qualification."""
import json
from types import SimpleNamespace

import pytest

from stream_core.notifications import measurement_continuity as policy

IDENT = 'reliability:upload_ceiling_coverage_or_freshness'


def tick(state, t, fresh, *, active=None, recovered=False, wall=0, boot='boot', items=True):
    item={'id':IDENT,'severity':'warning','summary':'measurement missing','observed_ts':wall+t}
    return policy.reconcile([item] if items else [], active or {}, state,
        now=SimpleNamespace(mono=t,wall=wall+t,boot=boot),
        conditions={IDENT:{'fresh':fresh,'recovered':recovered}})


def test_one_lost_check_and_recovery_warmup_never_open_an_incident():
    state={}
    assert not tick(state,0,False)
    for t in range(60,961,60):assert not tick(state,t,True,recovered=t>=960)


def test_sustained_and_recurring_loss_are_both_detected():
    state={}
    for t in (0,60,120):assert not tick(state,t,False)
    assert tick(state,180,False)[0]['id']==IDENT
    state={};first=None
    for t in range(0,601,60):
        result=tick(state,t,t%120!=0)
        if result and first is None:first=t
    assert first==360


@pytest.mark.parametrize('wall',[0,1e9,-1e9])
def test_wall_steps_do_not_affect_admission_and_process_restart_keeps_elapsed(wall):
    state={}
    for t in (0,60,120):assert not tick(state,t,False,wall=wall)
    restored=json.loads(json.dumps(state))
    assert tick(restored,180,False,wall=-wall)


def test_stable_good_resets_the_bad_budget():
    state={}
    for t in (0,60,120):tick(state,t,False)
    for t in (180,240,300,360):assert not tick(state,t,True)
    assert not tick(state,420,False)


def test_open_episode_keeps_last_bad_time_until_its_full_recovery_proof():
    state={};active={IDENT:{'last_bad_ts':42,'last_incident':{'id':IDENT,'summary':'previous','severity':'warning'}}}
    held=tick(state,500,True,active=active,recovered=False)
    assert held[0]['observed_ts']==42 and held[0]['_observation_pending']
    assert not tick(state,560,True,active=active,recovered=True)


def test_missing_owner_output_and_reboot_do_not_close_an_open_episode():
    state={};active={IDENT:{'last_bad_ts':42,'last_incident':{'id':IDENT,'summary':'previous','severity':'warning'}}}
    for boot in ('boot','newboot'):
        held=tick(state,1,False,active=active,items=False,boot=boot)
        assert held[0]['summary']=='previous' and held[0]['observed_ts']==42


def test_duplicate_decisions_and_unobserved_gaps_do_not_earn_elapsed_time():
    state={}
    for _ in range(10):assert not tick(state,0,False)
    assert not tick(state,1000,False)


@pytest.mark.parametrize('bad',[None,[],{'boot':'boot','points':[[float('nan'),True]]},
    {'boot':'boot','points':[[100,True],[50,False]]}])
def test_corrupt_continuity_is_bounded_and_cannot_create_a_false_incident(bad):
    state={'measurement_continuity':{IDENT:bad}}
    assert not tick(state,120,False)


def test_historical_slo_incidents_are_not_debounced_as_current_measurement_loss():
    item={'id':'reliability:upload_ceiling_historical_coverage','severity':'warning'}
    assert policy.reconcile([item],{}, {},now=SimpleNamespace(mono=0,boot='boot'))==[item]
