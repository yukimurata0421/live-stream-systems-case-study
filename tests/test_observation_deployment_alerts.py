import os,subprocess,json
from pathlib import Path
import pytest


@pytest.mark.parametrize('case',['healthy','drift','stopped','missing'])
def test_manifest_and_target_loss_alerts_with_real_promql(tmp_path,case):
    tool=os.environ.get('PROMTOOL')
    if not tool:pytest.skip('native promtool required')
    root=Path(__file__).resolve().parents[1]
    text=(root/'ops/monitoring/observation/alerts.yml.in').read_text()
    for name,value in {'UPLOAD_INSTANCE':'upload:9120','MONITOR_INSTANCE':'monitor:9126',
                       'UPLOAD_SOURCE_HASH':'a'*64,'UPLOAD_CONFIG_HASH':'b'*64,
                       'MONITOR_SOURCE_HASH':'c'*64,'MONITOR_CONFIG_HASH':'d'*64}.items():text=text.replace('@'+name+'@',value)
    rules=tmp_path/'rules.yml';rules.write_text(text)
    rows=[{'series':'up{job="stream_v3_monitor_observation",instance="monitor:9126"}','values':'1+0x30'},
          {'series':'stream_v3_observer_build_info{job="stream_v3_monitor_observation",instance="monitor:9126",source_hash="'+('c'*64)+'",config_hash="'+('d'*64)+'"}','values':'1+0x30'}]
    if case!='missing':rows.append({'series':'up{job="stream_v3_observation",instance="upload:9120"}','values':('0'if case=='stopped'else'1')+'+0x30'})
    if case=='healthy':rows.append({'series':'stream_v3_observer_build_info{job="stream_v3_observation",instance="upload:9120",source_hash="'+('a'*64)+'",config_hash="'+('b'*64)+'"}','values':'1+0x30'})
    expected=[]
    if case!='healthy':
        name='HostObservationBuildMismatch'if case=='drift'else'HostObservationExporterUnavailable'
        summary='The upload observer differs from the deployed manifest'if case=='drift'else'A direct observation exporter cannot be scraped'
        expected=[{'exp_labels':{'job':'stream_v3_observation','instance':'upload:9120','severity':'warning','scope':'observation'},'exp_annotations':{'summary':summary}}]
    tests=[]
    for alert in ('HostObservationBuildMismatch','HostObservationExporterUnavailable'):
        tests.append({'eval_time':'6m','alertname':alert,'exp_alerts':expected if case!='healthy'and alert==name else []})
    config={'rule_files':[str(rules)],'evaluation_interval':'15s','tests':[{'interval':'15s','input_series':rows,'alert_rule_test':tests}]}
    path=tmp_path/'test.yml';path.write_text(json.dumps(config))
    result=subprocess.run([tool,'test','rules',str(path)],capture_output=True,text=True,timeout=10)
    assert result.returncode==0,result.stdout+result.stderr
