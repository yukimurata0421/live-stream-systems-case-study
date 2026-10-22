"""Twitch alert delivery: one message on start, repeats while firing, one on resolve; other alerts ignored."""
import unittest

from stream_monitoring_v4.compatibility.twitch_alert_notify import decide, plan, deliver_plan, send
from datetime import datetime, timezone
from unittest.mock import patch

T0 = 1_800_000_000.0


def alert(name, state='firing', active='2027-01-15T08:00:00Z', **labels):
    return {'labels': {'alertname': name, **labels}, 'state': state, 'activeAt': active}


def delivery(now, state='HEALTHY', *, observed=None, api='LIVE'):
    at = datetime.fromtimestamp(now if observed is None else observed, timezone.utc).isoformat()
    return {'schema':'stream.relay_public.v1','generated_at':now,'source_fresh':True,'observed_at':at,
            'relay':{'status':'RUNNING'},'input':{'status':'FLOWING'},
            'destinations':{'twitch':{'status':'FLOWING'}},
            'twitch_api':{'state':api,'source_fresh':api!='UNKNOWN','observed_at':at},
            'twitch_delivery':{'state':state}}


class DecideTests(unittest.TestCase):
    def test_new_firing_alert_sends_once(self):
        msgs, state = decide([alert('TwitchUnexpectedOffline')], {}, T0, repeat_sec=3600)
        self.assertEqual(len(msgs), 1)
        self.assertIn('API and OCI output disagree', msgs[0])
        msgs, state = decide([alert('TwitchUnexpectedOffline')], state, T0 + 60, repeat_sec=3600)
        self.assertEqual(msgs, [])

    def test_repeat_while_firing_and_resolve(self):
        _, state = decide([alert('TwitchRelayNotProgressing')], {}, T0, repeat_sec=3600)
        msgs, state = decide([alert('TwitchRelayNotProgressing')], state, T0 + 3600, repeat_sec=3600)
        self.assertEqual(len(msgs), 1)
        self.assertIn('Still firing', msgs[0])
        msgs, state = decide([], state, T0 + 3700, repeat_sec=3600, delivery=delivery(T0+3700))
        self.assertEqual(msgs, [])
        msgs, state = decide([], state, T0 + 3760, repeat_sec=3600, delivery=delivery(T0+3760))
        self.assertEqual(len(msgs), 1)
        self.assertIn('Resolved', msgs[0])
        self.assertEqual(state, {})

    def test_pending_and_unrelated_alerts_are_ignored(self):
        msgs, state = decide([alert('TwitchUnexpectedOffline', state='pending'), alert('StreamV3NotifyActiveIncident')], {}, T0, repeat_sec=3600)
        self.assertEqual((msgs, state), ([], {}))

    def test_slo_alerts_are_tracked_per_sli(self):
        msgs, state = decide([alert('StreamV4TwitchSloBreached', sli='provider_live', window='7d'),
                              alert('StreamV4TwitchSloBreached', sli='daily_renewal', window='7d')], {}, T0, repeat_sec=3600)
        self.assertEqual(len(msgs), 2)
        self.assertEqual(len(state), 2)
        self.assertTrue(any('daily_renewal' in m for m in msgs))

    def test_api_warning_explains_collection_gaps_on_start_repeat_and_resolution(self):
        a = alert('StreamV4TwitchSloBreached', sli='api_measurement', window='7d', severity='warning')
        msgs, state = decide([a], {}, T0, repeat_sec=3600)
        self.assertTrue(msgs[0].startswith('\U0001f7e0'))
        self.assertIn('API observation', msgs[0])
        self.assertIn('missing', msgs[0])
        msgs, state = decide([a], state, T0+86400, repeat_sec=3600)
        self.assertIn('API observation', msgs[0])
        self.assertIn('missing', msgs[0])
        # A stored pre-upgrade state still gets the precise resolution title.
        del state[next(iter(state))]['title']
        msgs, state = decide([], state, T0+86430, repeat_sec=3600)
        self.assertIn('Resolved: Twitch API observation', msgs[0])

    def test_upgrade_keeps_active_alert_identity_without_duplicate_start(self):
        a = alert('StreamV4TwitchSloBreached', sli='api_measurement', window='7d')
        _, state = decide([a], {}, T0, repeat_sec=3600)
        del state[next(iter(state))]['title']
        msgs, after = decide([a], state, T0+30, repeat_sec=3600)
        self.assertEqual(msgs, [])
        self.assertEqual(set(after), set(state))

    def test_historical_slo_reminds_only_after_24_hours_for_each_sli(self):
        import json
        for sli in ('api_measurement', 'provider_live', 'relay_flow', 'daily_renewal'):
            with self.subTest(sli=sli):
                a = alert('StreamV4TwitchSloBreached', sli=sli, window='7d', severity='warning')
                msgs, state = plan([a], {}, T0, repeat_sec=3600, delivery=delivery(T0))
                self.assertEqual(len(msgs), 1)
                state = json.loads(json.dumps(state))  # persistent state survives a restart
                for elapsed in (3600, 23*3600, 86399):
                    msgs, state = plan([a], state, T0+elapsed, repeat_sec=3600, delivery=delivery(T0+elapsed))
                    self.assertEqual(msgs, [])
                msgs, state = plan([a], state, T0+86400, repeat_sec=3600, delivery=delivery(T0+86400))
                self.assertEqual(len(msgs), 1)
                self.assertEqual(msgs[0]['channel'], 'discord')
                self.assertIn('Still firing', msgs[0]['content'])
                msgs, state = plan([], state, T0+86430, repeat_sec=3600, delivery=delivery(T0+86430))
                self.assertEqual(len(msgs), 1)
                self.assertIn('Resolved', msgs[0]['content'])
                self.assertEqual(state, {})

    def test_legacy_slo_state_keeps_notification_and_incident_times(self):
        a = alert('StreamV4TwitchSloBreached', sli='api_measurement', window='7d', severity='warning')
        _, state = plan([a], {}, T0, repeat_sec=3600)
        k = next(iter(state))
        del state[k]['notification_status']
        before = dict(state[k])
        msgs, state = plan([a], state, T0+7200, repeat_sec=3600, delivery=delivery(T0+7200))
        self.assertEqual(msgs, [])
        self.assertEqual(state[k]['since'], before['since'])
        self.assertEqual(state[k]['last_sent'], before['last_sent'])
        self.assertEqual(state[k]['notification_status']['delivery_state'], 'HEALTHY')

    def test_slo_state_change_sends_immediately_and_restarts_reminder_interval(self):
        a = alert('StreamV4TwitchSloBreached', sli='api_measurement', window='7d', severity='warning')
        _, state = plan([a], {}, T0, repeat_sec=3600, delivery=delivery(T0))
        msgs, candidate = plan([a], state, T0+30, repeat_sec=3600,
                               delivery=delivery(T0+30, 'API_UNAVAILABLE', api='UNKNOWN'))
        self.assertEqual(len(msgs), 1)
        self.assertIn('State changed', msgs[0]['content'])
        self.assertIn('API: UNKNOWN', msgs[0]['content'])
        self.assertEqual(deliver_plan(msgs, candidate, state, lambda m: False), state)
        msgs, state = plan([a], state, T0+60, repeat_sec=3600,
                           delivery=delivery(T0+60, 'API_UNAVAILABLE', api='UNKNOWN'))
        self.assertEqual(len(msgs), 1)  # failed state-change delivery is retried
        msgs, state = plan([a], state, T0+90, repeat_sec=3600, delivery=delivery(T0+90))
        self.assertEqual(len(msgs), 1)
        self.assertIn('State changed', msgs[0]['content'])
        self.assertNotIn('Resolved:', msgs[0]['content'])  # historical SLO is still breached
        msgs, state = plan([a], state, T0+86400, repeat_sec=3600, delivery=delivery(T0+86400))
        self.assertEqual(msgs, [])
        msgs, state = plan([a], state, T0+86490, repeat_sec=3600, delivery=delivery(T0+86490))
        self.assertIn('Still firing', msgs[0]['content'])

    def test_slo_severity_changes_do_not_wait_for_daily_reminder(self):
        warning = alert('StreamV4TwitchSloBreached', sli='api_measurement', window='7d', severity='warning')
        critical = alert('StreamV4TwitchSloBreached', sli='api_measurement', window='7d', severity='critical')
        _, state = plan([warning], {}, T0, repeat_sec=3600, delivery=delivery(T0))
        msgs, state = plan([critical], state, T0+30, repeat_sec=3600, delivery=delivery(T0+30))
        self.assertEqual(msgs[0]['channel'], 'slack')
        msgs, state = plan([warning], state, T0+60, repeat_sec=3600, delivery=delivery(T0+60))
        self.assertIn('State changed', msgs[0]['content'])
        msgs, state = plan([], state, T0+90, repeat_sec=3600, delivery=delivery(T0+90))
        self.assertEqual(msgs[0]['channel'], 'slack')  # resolution follows the alerted route

    def test_current_path_and_api_failures_still_notify_and_repeat_hourly(self):
        slo = alert('StreamV4TwitchSloBreached', sli='api_measurement', window='7d', severity='warning')
        path = alert('TwitchRelayNotProgressing', severity='critical')
        api = alert('TwitchObservationUnavailable', severity='warning')
        _, state = plan([slo], {}, T0, repeat_sec=3600)
        msgs, state = plan([slo, path, api], state, T0+30, repeat_sec=3600)
        self.assertEqual({(m['key'], m['channel']) for m in msgs},
                         {('TwitchRelayNotProgressing', 'slack'), ('TwitchObservationUnavailable', 'discord')})
        msgs, state = plan([slo, path, api], state, T0+3630, repeat_sec=3600)
        self.assertEqual(len(msgs), 2)
        self.assertTrue(all('Still firing' in m['content'] for m in msgs))

    def test_slo_interval_is_independent_of_current_failure_interval(self):
        a = alert('StreamV4TwitchSloBreached', sli='api_measurement', window='7d')
        _, state = plan([a], {}, T0, repeat_sec=30, slo_repeat_sec=7200)
        msgs, state = plan([a], state, T0+30, repeat_sec=30, slo_repeat_sec=7200)
        self.assertEqual(msgs, [])
        msgs, state = plan([a], state, T0+7200, repeat_sec=30, slo_repeat_sec=7200)
        self.assertEqual(len(msgs), 1)

    def test_critical_goes_to_slack_and_waits_for_two_distinct_healthy_samples(self):
        a=alert('TwitchRelayNotProgressing',severity='critical')
        msgs,state=plan([a],{},T0,repeat_sec=3600,delivery=delivery(T0,'OCI_INPUT_STOPPED'))
        self.assertEqual(msgs[0]['channel'],'slack')
        msgs,state=plan([],state,T0+60,repeat_sec=3600,delivery=None)
        self.assertEqual(msgs,[]);self.assertTrue(state)
        msgs,state=plan([],state,T0+120,repeat_sec=3600,delivery=delivery(T0+120,api='UNKNOWN'))
        self.assertEqual(msgs,[]);self.assertTrue(state)
        msgs,state=plan([],state,T0+180,repeat_sec=3600,delivery=delivery(T0+180))
        self.assertEqual(msgs,[])
        msgs,state=plan([],state,T0+210,repeat_sec=3600,delivery=delivery(T0+210,observed=T0+180))
        self.assertEqual(msgs,[])  # rereading the same sample is not a second proof
        msgs,state=plan([],state,T0+240,repeat_sec=3600,delivery=delivery(T0+240))
        self.assertEqual(msgs[0]['channel'],'slack');self.assertIn('Resolved',msgs[0]['content'])
        self.assertEqual(state,{})

    def test_warning_and_historical_slo_include_oci_context_and_use_discord(self):
        alerts=[alert('TwitchObservationUnavailable',severity='warning'),
                alert('StreamV4TwitchSloBreached',sli='api_measurement',window='7d',severity='warning')]
        msgs,state=plan(alerts,{},T0,repeat_sec=3600,delivery=delivery(T0,'API_UNAVAILABLE',api='UNKNOWN'))
        self.assertTrue(all(m['channel']=='discord' for m in msgs))
        self.assertTrue(all('OCI relay: running / Input: progressing / Twitch output: progressing' in m['content'] for m in msgs))
        self.assertTrue(all('API: UNKNOWN' in m['content'] for m in msgs))
        msgs,after=plan([],state,T0+60,repeat_sec=3600,delivery=None)
        self.assertTrue(any(v['name']=='TwitchObservationUnavailable' for v in after.values()))

    def test_failed_discord_delivery_does_not_replay_an_acknowledged_slack_start(self):
        alerts=[alert('TwitchRelayNotProgressing',severity='critical'),
                alert('StreamV4TwitchSloBreached',sli='api_measurement',window='7d',severity='warning')]
        msgs,next_state=plan(alerts,{},T0,repeat_sec=3600)
        state=deliver_plan(msgs,next_state,{},lambda m:m['channel']=='slack')
        msgs,_=plan(alerts,state,T0+30,repeat_sec=3600)
        self.assertEqual([m['channel'] for m in msgs],['discord'])

    def test_stale_projection_cannot_close_a_critical_episode(self):
        _,state=plan([alert('TwitchRelayNotProgressing',severity='critical')],{},T0,repeat_sec=3600)
        msgs,after=plan([],state,T0+240,repeat_sec=3600,delivery=delivery(T0))
        self.assertEqual(msgs,[]);self.assertEqual(after,state)

    def test_channel_specific_wire_format(self):
        import json
        seen=[]
        class Response:
            status=200
            def __enter__(self):return self
            def __exit__(self,*args):pass
        def capture(req,**kwargs):seen.append(json.loads(req.data));return Response()
        with patch('urllib.request.urlopen',side_effect=capture):
            self.assertTrue(send('https://example.invalid','message','fixture','slack'))
            self.assertTrue(send('https://example.invalid','message','fixture','discord'))
        self.assertEqual(seen[0],{'text':'message'})
        self.assertEqual(seen[1],{'content':'message','username':'fixture'})

    def test_failed_initial_critical_delivery_cannot_create_an_unpaired_recovery(self):
        msgs,candidate=plan([alert('TwitchRelayNotProgressing',severity='critical')],{},T0,repeat_sec=3600)
        state=deliver_plan(msgs,candidate,{},lambda m:False)
        self.assertEqual(state,{})
        msgs,state=plan([],state,T0+60,repeat_sec=3600,delivery=delivery(T0+60))
        self.assertEqual(msgs,[])


if __name__ == '__main__':
    unittest.main()
