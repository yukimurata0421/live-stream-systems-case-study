#!/usr/bin/env python3
"""Render private, IPv4-only resolver input for the publisher mount namespace."""
import argparse
import hashlib
import ipaddress
import json
from pathlib import Path


def render(raw):
    if len(raw) > 8192:
        raise ValueError('resolver_config_too_large')
    current = []
    for line in raw.decode().splitlines():
        parts = line.split('#', 1)[0].split()
        if len(parts) == 2 and parts[0] == 'nameserver':
            address = ipaddress.ip_address(parts[1])
            if address.version == 4:
                current.append(str(address))
        elif parts and (parts[0] != 'options' or any(not x.startswith(('timeout:', 'attempts:')) for x in parts[1:])):
            # Preserve search/domain semantics only after explicit review.
            raise ValueError('resolver_search_configuration_requires_review')
    servers = list(dict.fromkeys([*current[:1], '1.1.1.1', '8.8.8.8']))
    return ('# Publisher-only resolver; generated from the current IPv4 primary.\n'
            + ''.join(f'nameserver {server}\n' for server in servers)
            + 'options timeout:1 attempts:2\n').encode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, default=Path('/etc/resolv.conf'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    raw = render(args.source.read_bytes())
    with args.output.open('xb') as handle:
        handle.write(raw)
    print(json.dumps({'sha256': hashlib.sha256(raw).hexdigest(), 'ipv4_resolvers':
                      sum(line.startswith(b'nameserver') for line in raw.splitlines())}))


if __name__ == '__main__':
    main()
