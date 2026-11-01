#!/usr/bin/env python3
"""Create a minimal image build context; no state, credentials or unrelated modules."""
import argparse
from pathlib import Path

MODULES=('__init__.py','twitch_thumbnail.py','twitch_thumbnail_worker.py',
         'twitch_thumbnail_evidence.py','thumbnail_http.py','verified_clock.py',
         'viewer_clock.py','viewer_clock_capture.py','viewer_clock_worker.py')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output',type=Path,help='New, isolated build-context directory')
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[3]
    args.output.mkdir(parents=True,exist_ok=False)
    target=args.output/'src/stream_v3';target.mkdir(parents=True)
    for name in MODULES:(target/name).write_bytes((root/'src/stream_v3'/name).read_bytes())
    (args.output/'Dockerfile').write_bytes(Path(__file__).with_name('Dockerfile').read_bytes())


if __name__=='__main__':main()
