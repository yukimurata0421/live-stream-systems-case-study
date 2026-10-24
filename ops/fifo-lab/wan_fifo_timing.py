#!/usr/bin/env python3
"""Measure FIFO recovery across an isolated WAN blackout and outer-address change."""
from __future__ import annotations

import argparse
import copy
import json
import time

from nvenc_transport_timing import NvencTransportLab

TARGETS = ('lan_gateway', 'external_isolated', 'oci_outer', 'oci_wg')


class WanFifoLab(NvencTransportLab):
    tasks_max = 128
    sender_mode = 'fifo'
    ping_interval_sec = .5

    def before_wan_fault(self):
        pass

    def command(self, role, **value):
        if value.get('op') == 'start':
            value.update(ping_interval_sec=self.ping_interval_sec, report_pings=True)
        super().command(role, **value)

    def request(self, role, kind, **value):
        sent = time.monotonic()
        self.command(role, **value)
        return self.wait(lambda e: e['kind'] == kind and e['role'] == role
                         and e['controller_received_mono'] > sent, 5)

    def start(self):
        super().start()
        self.request('oci', 'wan_probes_configured', op='configure_wan_probes')
        self.request('dell', 'wan_probes_configured', op='configure_wan_probes')

    def run_wan_case(self, outage):
        case = f'{self.sender_mode}-wan-ipv4-{outage:02d}s' + getattr(self, 'case_suffix', '')
        self.request('dell', 'ip_changed', op='ip', address='192.0.2.2')
        self.marker('wan_case_begin', case=case, outage_sec=outage)
        self.command('oci', op='start', case=case)
        self.wait(lambda e: e['kind'] == 'server_started' and e.get('case') == case)
        self.duration(.3)
        self.command('dell', op='start', case=case, mode=self.sender_mode)
        started = self.wait(lambda e: e['kind'] == 'sender_started' and e.get('case') == case)
        self.wait(lambda e: e['kind'] == 'transport_probe' and e.get('case') == case
                  and e['stage'] == 'rtmps_publish_ack', 15)
        self.wait(lambda e: e['kind'] == 'trace' and e.get('case') == case
                  and e.get('stage') == 'rtmp_publish', 15)
        self.validate_media(case, started['pid'], started['mono'])
        self.duration(1)
        before = self.request('dell', 'tcp_snapshot', op='tcp_snapshot')
        baseline = time.monotonic() - 2
        for target in TARGETS:
            self.wait(lambda e: e['kind'] == 'pong' and e.get('case') == case
                      and e.get('target') == target and e['sent_mono'] >= baseline, 5)
        self.before_wan_fault()
        counts_before = copy.deepcopy(self.packet_counts)
        self.drop = True
        down = self.marker('wan_down', case=case, outage_sec=outage,
                           packet_counts_before=counts_before)
        self.duration(outage / 2)
        changed = self.request('dell', 'ip_changed', op='ip', address='198.51.100.2')
        remaining = max(0, down + outage - time.monotonic())
        self.duration(remaining)
        counts_at_restore = copy.deepcopy(self.packet_counts)
        self.drop = False
        restored = self.marker('wan_restored', case=case, packet_counts=counts_at_restore)
        ping_recovery = {}
        for target in TARGETS:
            pong = self.wait(lambda e: e['kind'] == 'pong' and e.get('case') == case
                             and e.get('target') == target and e['sent_mono'] >= restored, 10)
            ping_recovery[target] = pong['mono'] - restored
        # Natural write errors/timeouts only; no ss -K or signals during recovery.
        ack = self.wait(lambda e: e['kind'] == 'transport_probe' and e.get('case') == case
                        and e['stage'] == 'rtmps_publish_ack' and e['mono'] >= restored
                        and (e['connection'] > 1 if self.sender_mode == 'fifo'
                             else e['app_pid'] != started['pid']),
                        max(1, restored + 45 - time.monotonic()))
        fifo = None
        if self.sender_mode == 'fifo':
            fifo = self.wait(lambda e: e['kind'] == 'trace' and e.get('case') == case
                             and e.get('stage') == 'fifo_recovery_success'
                             and e['mono'] >= restored, 10)
        self.wait(lambda e: e['kind'] == 'trace' and e.get('case') == case
                  and e.get('stage') == 'rtmp_publish' and self.local_time(e) > restored, 15)
        media = self.validate_media(case, ack['app_pid'], restored)
        media['first_video_mux_after_restore_sec'] = media.pop('first_video_mux_after_error_sec')
        media['first_audio_mux_after_restore_sec'] = media.pop('first_audio_mux_after_error_sec')
        fresh = self.request('dell', 'tcp_snapshot', op='tcp_snapshot')
        self.duration(1)
        later = self.request('dell', 'tcp_snapshot', op='tcp_snapshot')
        status = self.request('oci', 'status', op='status')
        assert '198.51.100.2:51823' in status['endpoints']
        assert changed['mono'] > down and changed['mono'] < restored
        assert before['pid'] == started['pid']
        assert fresh['pid'] == later['pid'] == ack['app_pid']
        assert fresh['cookie'] != before['cookie'] and later['bytes_acked'] > fresh['bytes_acked']
        case_events = [e for e in self.events if e.get('case') == case]
        starts = [e for e in case_events if e['kind'] == 'sender_started']
        exits = [e for e in case_events if e['kind'] == 'sender_exited']
        if self.sender_mode == 'fifo':
            assert len(starts) == 1 and not exits and fresh['pid'] == started['pid']
        else:
            assert 2 <= len(starts) <= 21 and len(exits) == len(starts) - 1
            assert fresh['pid'] != started['pid'] and starts[-1]['pid'] == fresh['pid']
            assert [e['generation'] for e in starts] == list(range(1, len(starts) + 1))
            assert all(e['mono'] >= down for e in exits)
        assert not any(e['kind'] in ('output_cut_begin', 'connection_reset_begin', 'restart_requested')
                       for e in case_events)
        pings = {}
        for target in TARGETS:
            sent = [e for e in case_events if e['kind'] == 'ping_sent' and e['target'] == target
                    and down + .25 <= e['sent_mono'] <= restored - .25]
            replies = [e for e in case_events if e['kind'] == 'pong' and e['target'] == target
                       and down + .25 <= e['sent_mono'] <= restored - .25]
            assert len(sent) >= outage - 3
            if target == 'lan_gateway':
                assert len(replies) == len(sent)
            else:
                assert not replies, 'isolated WAN must drop every remote ping during outage'
            pings[target] = {'sent_during_outage': len(sent), 'replies_during_outage': len(replies),
                             'first_post_restore_reply_sec': ping_recovery[target]}
        dropped = {role: counts_at_restore[role]['dropped'] - counts_before[role]['dropped']
                   for role in ('dell', 'oci')}
        assert all(value > 0 for value in dropped.values())
        assert all(counts_at_restore[role]['forwarded'] == counts_before[role]['forwarded']
                   for role in ('dell', 'oci'))
        stages = {e['stage']: e for e in case_events if e['kind'] == 'transport_probe'
                  and e['connection'] == ack['connection'] and e['app_pid'] == ack['app_pid']
                  and e['stage'] != 'transport_error'}
        assert stages['tls_complete']['value'] == 0
        errors = [e for e in case_events if e['kind'] == 'transport_probe'
                  and e['stage'] == 'transport_error' and e['mono'] >= down]
        attempts = [e for e in case_events if e['kind'] == 'transport_probe'
                    and e['stage'] == 'tcp_begin' and e['mono'] >= down]
        result = {'case': case, 'outage_setting_sec': outage, 'actual_outage_sec': restored - down,
                  'sender_pid': started['pid'], 'recovered_sender_pid': fresh['pid'],
                  'mode': self.sender_mode, 'same_sender_pid': fresh['pid'] == started['pid'],
                  'sender_generations': len(starts),
                  'ipv4_changed_during_outage': True, 'oci_wg_endpoint_followed': True,
                  'restore_to_tcp_complete_sec': stages['tcp_complete']['mono'] - restored,
                  'restore_to_tls_complete_sec': stages['tls_complete']['mono'] - restored,
                  'restore_to_rtmps_ack_sec': ack['mono'] - restored,
                  'restore_to_fifo_success_sec': fifo['mono'] - restored if fifo else None,
                  'restore_to_first_transport_error_sec': errors[0]['mono'] - restored if errors else None,
                  'successful_tcp_attempt_started_before_restore': stages['tcp_begin']['mono'] < restored,
                  'tcp_attempts_since_outage': len(attempts),
                  'dropped_packets_during_outage': dropped, 'pings': pings,
                  'old_socket_cookie': before['cookie'], 'new_socket_cookie': fresh['cookie'],
                  'new_tcp_ack_bytes_advanced': later['bytes_acked'] - fresh['bytes_acked'],
                  'forced_socket_cuts': 0, 'tls_session_resumed': False, 'decoder_started': False,
                  'outcome': ('WAN_IPV4_RECOVERED_BY_FIFO_SAME_FFMPEG_PID' if fifo else
                              'WAN_IPV4_RECOVERED_BY_FFMPEG_RESTART'), **media}
        if self.sender_mode == 'direct':
            schedules = [e for e in case_events if e['kind'] == 'restart_scheduled']
            assert len(schedules) == len(exits)
            gaps = [b['mono'] - a['mono'] for a, b in zip(exits, starts[1:])]
            assert schedules[0]['restart_wait_sec'] == 0
            assert all(e['restart_wait_sec'] == 1 for e in schedules[1:])
            result.update(restarts=len(exits), restart_gaps_sec=gaps,
                          initial_restart_wait_sec=0, failed_restart_retry_wait_sec=1,
                          max_restarts=20,
                          restore_to_original_exit_sec=exits[0]['mono'] - restored,
                          restore_to_successful_spawn_sec=starts[-1]['mono'] - restored,
                          original_error_to_exit_sec=exits[0]['mono'] - errors[0]['mono'] if errors else None,
                          successful_spawn_to_tcp_begin_sec=stages['tcp_begin']['mono'] - starts[-1]['mono'],
                          successful_spawn_to_publish_ack_sec=ack['mono'] - starts[-1]['mono'],
                          tcp_begin_to_tcp_complete_sec=stages['tcp_complete']['mono'] - stages['tcp_begin']['mono'],
                          tcp_complete_to_tls_complete_sec=stages['tls_complete']['mono'] - stages['tcp_complete']['mono'],
                          tls_complete_to_publish_ack_sec=ack['mono'] - stages['tls_complete']['mono'])
        self.results.append(result)
        (self.root / 'results.json').write_text(json.dumps(self.results, indent=2) + '\n')
        self.marker('wan_case_result', **result)
        print(json.dumps(result), flush=True)
        self.command('dell', op='stop')
        self.command('oci', op='stop')
        self.duration(.7)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--guard-observation', required=True, help='Read-only runtime observation to protect during the experiment')
    p.add_argument('--local-root', required=True)
    p.add_argument('--remote-root', required=True)
    p.add_argument('--unit', required=True)
    p.add_argument('--mediamtx', required=True)
    p.add_argument('--outages', nargs='+', type=int, choices=(12, 30, 60), default=(12, 30, 60))
    args = p.parse_args()
    if len(set(args.outages)) != len(args.outages):
        p.error('outage cases must be unique within a batch')
    lab = WanFifoLab(args)
    try:
        lab.start()
        for outage in args.outages:
            lab.run_wan_case(outage)
    except Exception as exc:
        lab.marker('failure', error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        lab.close()


if __name__ == '__main__':
    main()
