from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

try:
    from .identity import TargetIdentity
    from .sources import EvidenceRecord, SourceKind
except ImportError:
    from identity import TargetIdentity  # type: ignore
    from sources import EvidenceRecord, SourceKind  # type: ignore


@dataclass(frozen=True)
class LedgerSnapshot:
    latest_by_source: dict[SourceKind, EvidenceRecord]
    recent_records: tuple[EvidenceRecord, ...]
    current_target_epoch: int
    current_restart_epoch: int
    last_restart_at: float
    canonical_target: TargetIdentity
    taken_at: float


class EvidenceLedger:
    def __init__(self, state_path: str | Path, *, recent_limit: int = 64):
        self._path = Path(state_path).expanduser()
        self._recent_limit = max(1, int(recent_limit))
        self._latest_by_source: dict[SourceKind, EvidenceRecord] = {}
        self._recent_records: list[EvidenceRecord] = []
        self._current_target_epoch = 0
        self._current_restart_epoch = 0
        self._last_restart_at = 0.0
        self._canonical_target = TargetIdentity()
        self._load()

    @property
    def current_target_epoch(self) -> int:
        return self._current_target_epoch

    @property
    def current_restart_epoch(self) -> int:
        return self._current_restart_epoch

    @property
    def canonical_target(self) -> TargetIdentity:
        return self._canonical_target

    def ensure_target(self, target: TargetIdentity) -> TargetIdentity:
        if target.video_id and target.video_id != self._canonical_target.video_id:
            self._current_target_epoch += 1
            self._canonical_target = TargetIdentity(
                channel_id=target.channel_id or self._canonical_target.channel_id,
                video_id=target.video_id,
                broadcast_id=target.broadcast_id,
                bound_stream_id=target.bound_stream_id,
                target_epoch=self._current_target_epoch,
                restart_epoch=self._current_restart_epoch,
            )
            self._persist()
        elif target.channel_id and target.channel_id != self._canonical_target.channel_id:
            self._canonical_target = TargetIdentity(
                channel_id=target.channel_id,
                video_id=self._canonical_target.video_id,
                broadcast_id=self._canonical_target.broadcast_id,
                bound_stream_id=self._canonical_target.bound_stream_id,
                target_epoch=self._current_target_epoch,
                restart_epoch=self._current_restart_epoch,
            )
            self._persist()
        return self._canonical_target

    def record(self, ev: EvidenceRecord) -> None:
        if ev.target_epoch < self._current_target_epoch or ev.restart_epoch < self._current_restart_epoch:
            return
        self._latest_by_source[ev.source] = ev
        self._recent_records.append(ev)
        if len(self._recent_records) > self._recent_limit:
            self._recent_records = self._recent_records[-self._recent_limit :]
        self._persist()

    def snapshot(self) -> LedgerSnapshot:
        return LedgerSnapshot(
            latest_by_source=dict(self._latest_by_source),
            recent_records=tuple(self._recent_records),
            current_target_epoch=self._current_target_epoch,
            current_restart_epoch=self._current_restart_epoch,
            last_restart_at=self._last_restart_at,
            canonical_target=self._canonical_target,
            taken_at=time.time(),
        )

    def bump_restart_epoch(self, *, now_ts: float | None = None) -> int:
        now = time.time() if now_ts is None else float(now_ts)
        self._current_restart_epoch += 1
        self._last_restart_at = now
        self._canonical_target = TargetIdentity(
            channel_id=self._canonical_target.channel_id,
            video_id=self._canonical_target.video_id,
            broadcast_id=self._canonical_target.broadcast_id,
            bound_stream_id=self._canonical_target.bound_stream_id,
            target_epoch=self._current_target_epoch,
            restart_epoch=self._current_restart_epoch,
        )
        self._persist()
        return self._current_restart_epoch

    def _load(self) -> None:
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        self._current_target_epoch = int(payload.get("current_target_epoch", 0) or 0)
        self._current_restart_epoch = int(payload.get("current_restart_epoch", 0) or 0)
        self._last_restart_at = float(payload.get("last_restart_at", 0) or 0)
        self._canonical_target = TargetIdentity.from_dict(payload.get("canonical_target"))
        latest: dict[SourceKind, EvidenceRecord] = {}
        for key, item in (payload.get("latest_by_source", {}) or {}).items():
            if not isinstance(item, dict):
                continue
            try:
                ev = EvidenceRecord.from_dict(item)
                latest[SourceKind(str(key))] = ev
            except Exception:
                continue
        self._latest_by_source = latest
        recent: list[EvidenceRecord] = []
        for item in payload.get("recent_records", []) or []:
            if not isinstance(item, dict):
                continue
            try:
                recent.append(EvidenceRecord.from_dict(item))
            except Exception:
                continue
        self._recent_records = recent[-self._recent_limit :]

    def _persist(self) -> None:
        payload = {
            "current_target_epoch": self._current_target_epoch,
            "current_restart_epoch": self._current_restart_epoch,
            "last_restart_at": self._last_restart_at,
            "canonical_target": self._canonical_target.to_dict(),
            "latest_by_source": {k.value: v.to_dict() for k, v in self._latest_by_source.items()},
            "recent_records": [ev.to_dict() for ev in self._recent_records[-self._recent_limit :]],
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")
        tmp.replace(self._path)
