"""Exercise the real SLI producer and the integrated public consumer together."""
import importlib.util
from pathlib import Path

from stream_monitoring_v4.compatibility import twitch_sli


def test_history_projection_is_accepted_by_the_public_publisher(tmp_path):
    root = Path(__file__).resolve().parents[1]
    candidates = [root.parent/'ops/public-publisher/site/scripts/collect_twitch_sli.py',
                  root.parent/'stream_v3/ops/public-publisher/site/scripts/collect_twitch_sli.py']
    source = next(path for path in candidates if path.is_file())
    spec = importlib.util.spec_from_file_location('integrated_sli_publisher', source)
    publisher = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(publisher)
    started = 1_800_000_000.
    history = twitch_sli.History(tmp_path/'history.sqlite3', started)
    try:
        history.tick(started+30, {'observed': started+30, 'api':'GOOD', 'live':'GOOD',
                                 'stream':'fixture-stream', 'started':twitch_sli.iso(started)},
                     {'observed':started+30, 'flow':'GOOD'})
        snapshot = history.public(started+60)
        assert publisher.validate(snapshot, started+61) is snapshot
        metric = next(m for m in snapshot['windows'][0]['metrics'] if m['id']=='api_measurement')
        assert metric['good_minutes'] == 1
        assert metric['missing_minutes'] == 1439
        assert snapshot['timeline'][-1]['good'] == 1
    finally:
        history.db.close()
