from __future__ import annotations

import re


def latency_usec_from_line(line: str, prefix: str) -> int:
    m = re.search(rf"^{re.escape(prefix)}:\s*(\d+)\s*usec", line.strip())
    if not m:
        return -1
    try:
        return int(m.group(1))
    except Exception:
        return -1


def parse_pactl_entries(raw: str, header_prefix: str) -> list[dict]:
    entries: list[dict] = []
    current: dict | None = None
    in_props = False
    for line in raw.splitlines():
        if line.startswith(header_prefix):
            if current is not None:
                entries.append(current)
            entry_id = -1
            m = re.search(r"#(\d+)", line)
            if m:
                try:
                    entry_id = int(m.group(1))
                except Exception:
                    entry_id = -1
            current = {
                "id": entry_id,
                "buffer_latency_usec": -1,
                "peer_latency_usec": -1,
                "properties": {},
            }
            in_props = False
            continue
        if current is None:
            continue
        stripped = line.strip()
        if not stripped:
            continue
        if stripped == "Properties:":
            in_props = True
            continue
        if in_props and "=" in stripped:
            key, val = stripped.split("=", 1)
            value = val.strip()
            if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
                value = value[1:-1]
            current["properties"][key.strip()] = value
            continue
        lat = latency_usec_from_line(stripped, "Buffer Latency")
        if lat >= 0:
            current["buffer_latency_usec"] = lat
            continue
        sink_lat = latency_usec_from_line(stripped, "Sink Latency")
        if sink_lat >= 0:
            current["peer_latency_usec"] = sink_lat
            continue
        source_lat = latency_usec_from_line(stripped, "Source Latency")
        if source_lat >= 0:
            current["peer_latency_usec"] = source_lat
            continue
    if current is not None:
        entries.append(current)
    return entries
