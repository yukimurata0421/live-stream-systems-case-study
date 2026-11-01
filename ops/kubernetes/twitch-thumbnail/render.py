#!/usr/bin/env python3
"""Render the observation-only resources; never contact a cluster or copy credentials."""
import argparse
import ipaddress
import json
from pathlib import Path
import re


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',required=True,help='Imported image pinned by manifest digest')
    parser.add_argument('--source-gateway',required=True,help='Arena pod-network gateway address')
    parser.add_argument('--user-id',required=True)
    parser.add_argument('--login',required=True)
    parser.add_argument('--replicas',type=int,choices=(0,1),default=0)
    args=parser.parse_args()
    try:gateway=ipaddress.IPv4Address(args.source_gateway)
    except ipaddress.AddressValueError:parser.error('IPv4 source gateway required')
    if gateway.is_loopback or gateway.is_multicast or gateway.is_unspecified or not gateway.is_private:
        parser.error('source gateway must be a private, non-loopback unicast address')
    if not re.fullmatch(r'[a-zA-Z0-9./_:-]+@sha256:[0-9a-f]{64}',args.image):parser.error('image manifest digest required')
    if not re.fullmatch(r'[0-9]{1,64}',args.user_id) or not re.fullmatch(r'[a-z0-9_]{1,25}',args.login):parser.error('invalid target identity')
    text=Path(__file__).with_name('manifest.json').read_text()
    for key,value in {'__THUMBNAIL_IMAGE__':args.image,'__ARENA_POD_GATEWAY__':str(gateway),
                      '__TWITCH_USER_ID__':args.user_id,'__TWITCH_USER_LOGIN__':args.login}.items():text=text.replace(key,value)
    data=json.loads(text)
    for item in data['items']:
        if item['kind']=='Deployment':item['spec']['replicas']=args.replicas
    print(json.dumps(data,indent=2))


if __name__=='__main__':main()
