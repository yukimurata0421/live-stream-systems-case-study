"""Clock offsets, delay uncertainty, monotonic cache expiry and cold entrypoints."""
import copy
import importlib.util
import json
import pathlib
import subprocess
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1] / 'ops/selfheal'
spec = importlib.util.spec_from_file_location('clock_under_test', ROOT/'clock_contract.py')
clock = importlib.util.module_from_spec(spec); sys.modules[spec.name] = clock; spec.loader.exec_module(clock)
NONCE = 'a'*32
MACHINE = 'b'*64


def exchange(offset=0, forward=.03, backward=.07, work=.2):
    sent={'wall':1000.,'mono':100.}
    received={'wall':1000+forward+work+backward,'mono':100+forward+work+backward}
    value={'schema':clock.SCHEMA,'nonce':NONCE,'machine_id_sha256':MACHINE,'boot_id':'remote',
           'received':{'wall':1000+forward+offset,'mono':200.},
           'sent':{'wall':1000+forward+work+offset,'mono':200+work}}
    return value,sent,received


def model(value,sent,received):
    return clock.estimate(value,NONCE,sent,received,machine=MACHINE,boot_id='remote',local_boot='local')


class ClockTests(unittest.TestCase):
    def test_offset_bounds_cover_large_offsets_and_asymmetric_networks(self):
        for offset in (-600,-10,-.065,0,.065,10,600):
            for forward,backward in ((.001,.299),(.15,.15),(.299,.001)):
                with self.subTest(offset=offset,forward=forward):
                    p,a,b=exchange(offset,forward,backward);m=model(p,a,b)
                    self.assertLessEqual(m.low,offset);self.assertGreaterEqual(m.high,offset)
                    self.assertAlmostEqual(m.network_rtt,.3)
                    self.assertTrue(m.fresh(p['sent']['wall'],20,now=b))
                    self.assertFalse(m.fresh(p['sent']['wall']-21,20,now=b))
                    self.assertFalse(m.fresh(p['sent']['wall']+2,20,now=b))

    def test_nonce_machine_boot_and_nonfinite_are_rejected(self):
        for field,value in [('nonce','c'*32),('machine_id_sha256','d'*64),('boot_id','new-boot')]:
            p,a,b=exchange();p[field]=value
            with self.assertRaises(clock.ClockRejected):model(p,a,b)
        for bad in (True,float('nan'),float('inf'),'1000'):
            p,a,b=exchange();p['sent']['wall']=bad
            with self.assertRaises(clock.ClockRejected):model(p,a,b)

    def test_steps_stalls_and_impossible_durations_are_rejected(self):
        for mutation in ('local-step','remote-step','stalled','reverse'):
            p,a,b=exchange()
            if mutation=='local-step':b['wall']+=10
            if mutation=='remote-step':p['sent']['wall']-=10
            if mutation=='stalled':b['wall']+=13;b['mono']+=13
            if mutation=='reverse':p['sent']['mono']-=1
            with self.subTest(mutation=mutation),self.assertRaises(clock.ClockRejected):model(p,a,b)

    def test_cached_model_uncertainty_grows_and_wall_step_or_reboot_invalidates_it(self):
        p,a,b=exchange();m=model(p,a,b)
        later={'wall':b['wall']+100,'mono':b['mono']+100}
        lo,hi=m.bounds(now=later)
        self.assertAlmostEqual(lo,m.low-.1);self.assertAlmostEqual(hi,m.high+.1)
        later['wall']+=1
        with self.assertRaises(clock.ClockRejected):m.bounds(now=later)
        with self.assertRaises(clock.ClockRejected):m.bounds(now=b,local_identity={'boot_id':'new'})
        with self.assertRaises(clock.ClockRejected):m.bounds(now={'wall':b['wall']+181,'mono':b['mono']+181})

    def test_cached_data_does_not_rejuvenate_on_read_or_restart(self):
        p,a,b=exchange();m=model(p,a,b);cache=clock.EvidenceCache(20)
        cache.put({'status':'last-known'},m,p['sent']['wall'])
        self.assertIsNotNone(cache.get(now=b))
        self.assertIsNotNone(cache.get(now={'wall':b['wall']+18,'mono':b['mono']+18}))
        self.assertIsNone(cache.get(now={'wall':b['wall']+21,'mono':b['mono']+21}))
        self.assertIsNone(clock.EvidenceCache(20).get(now=b))

    def test_cold_isolated_read_only_probe_uses_real_kernel_abi(self):
        r=subprocess.run([sys.executable,'-I','-B',str(ROOT/'clock_observer.py'),'--host-id','isolated-test','--exchange',NONCE],
                         capture_output=True,timeout=5,check=True)
        value=json.loads(r.stdout)
        self.assertEqual(value['clock']['nonce'],NONCE)
        self.assertEqual(value['value']['status'],'OBSERVED')
        self.assertIsInstance(value['value']['synchronized'],bool)


if __name__=='__main__':unittest.main()
