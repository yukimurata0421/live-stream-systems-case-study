from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TargetIdentity:
    channel_id: str = ""
    video_id: str = ""
    broadcast_id: str = ""
    bound_stream_id: str = ""
    target_epoch: int = 0
    restart_epoch: int = 0

    def matches(self, other: "TargetIdentity") -> bool:
        if self.channel_id and other.channel_id and self.channel_id != other.channel_id:
            return False
        if self.video_id and other.video_id:
            return self.video_id == other.video_id
        if self.broadcast_id and other.broadcast_id:
            return self.broadcast_id == other.broadcast_id
        return True

    def strong_video_match(self, other: "TargetIdentity") -> bool:
        if self.channel_id and other.channel_id and self.channel_id != other.channel_id:
            return False
        return bool(self.video_id and other.video_id and self.video_id == other.video_id)

    def to_dict(self) -> dict:
        return {
            "channel_id": self.channel_id,
            "video_id": self.video_id,
            "broadcast_id": self.broadcast_id,
            "bound_stream_id": self.bound_stream_id,
            "target_epoch": self.target_epoch,
            "restart_epoch": self.restart_epoch,
        }

    @classmethod
    def from_dict(cls, payload: dict | None) -> "TargetIdentity":
        if not isinstance(payload, dict):
            return cls()
        return cls(
            channel_id=str(payload.get("channel_id", "") or ""),
            video_id=str(payload.get("video_id", "") or ""),
            broadcast_id=str(payload.get("broadcast_id", "") or ""),
            bound_stream_id=str(payload.get("bound_stream_id", "") or ""),
            target_epoch=int(payload.get("target_epoch", 0) or 0),
            restart_epoch=int(payload.get("restart_epoch", 0) or 0),
        )
