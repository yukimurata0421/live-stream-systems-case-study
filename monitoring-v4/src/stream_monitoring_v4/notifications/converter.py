from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.notification import NotificationIntent
from stream_contracts.monitoring_v4.time import parse_utc, utc_text


PHASE_MAP = {
    "detected": "detected",
    "status": "repeat",
    "repeat": "repeat",
    "recovered": "recovered",
    "auto_recovered": "recovered",
}


@dataclass(frozen=True)
class ConversionReport:
    converted: tuple[NotificationIntent, ...]
    rejected: tuple[str, ...]
    collisions: tuple[str, ...]


def _created_at(item: Mapping[str, Any]) -> str:
    raw = str(item.get("created_ts_utc") or item.get("updated_ts_utc") or "").strip()
    if raw:
        parse_utc(raw, field="v3 created timestamp")
        return raw
    timestamp = int(item.get("created_ts", 0) or 0)
    if timestamp <= 0:
        raise ValueError("v3 outbox row has no valid created timestamp")
    return utc_text(timestamp)


def convert_v3_rows(rows: Iterable[Mapping[str, Any]]) -> ConversionReport:
    converted: list[NotificationIntent] = []
    rejected: list[str] = []
    collisions: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(rows):
        legacy_id = str(item.get("message_id", "")).strip()
        phase = PHASE_MAP.get(str(item.get("phase", "")).strip())
        content = str(item.get("content", "")).strip()
        route = str(item.get("route", "discord") or "discord")
        if not legacy_id or phase is None or not content:
            rejected.append(f"row={index}:missing_message_id_phase_or_content")
            continue
        try:
            created_at = _created_at(item)
            episode_id = stable_id("inc", "legacy-v3", legacy_id)
            transition_id = stable_id("trn", "legacy-v3", legacy_id, phase)
            intent = NotificationIntent.create(
                transition_id=transition_id,
                episode_id=episode_id,
                route=route,
                phase=phase,
                severity="warning" if phase != "recovered" else "info",
                created_at=created_at,
                not_before=created_at,
                subject=f"[legacy-v3] {phase}",
                content=content,
                route_policy_revision="legacy-v3-route-policy",
                template_revision="legacy-v3-content-preserved",
                dedupe_parts=("legacy-v3", legacy_id),
            )
        except (TypeError, ValueError) as exc:
            rejected.append(f"row={index}:{type(exc).__name__}")
            continue
        if intent.intent_id in seen:
            collisions.append(legacy_id)
            continue
        seen.add(intent.intent_id)
        converted.append(intent)
    return ConversionReport(tuple(converted), tuple(rejected), tuple(collisions))


def reverse_to_v3_rows(intents: Iterable[NotificationIntent]) -> list[dict[str, Any]]:
    phase = {"detected": "detected", "repeat": "status", "recovered": "recovered"}
    return [
        {
            "message_id": intent.dedupe_key,
            "phase": phase[intent.phase],
            "incident_ids": [intent.episode_id],
            "content": intent.content,
            "username": "ADS-B Stream Watchdog",
            "route": intent.route,
            "status": "pending",
            "attempts": 0,
            "created_ts": int(parse_utc(intent.created_at).timestamp()),
            "created_ts_utc": intent.created_at,
            "updated_ts": int(parse_utc(intent.created_at).timestamp()),
            "updated_ts_utc": intent.created_at,
            "last_error": "",
        }
        for intent in intents
    ]
