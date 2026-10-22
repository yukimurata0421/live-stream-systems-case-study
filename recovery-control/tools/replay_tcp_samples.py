#!/usr/bin/env python3
"""Replay recorded TCP collector samples through the agent's stall detector.

Input is the read-only collector format (stream_v3.tcp_stall_raw_sample.v1, one JSON object
per line) on stdin or from files. Each sample's `source` is the runtime observation the agent
reads live, so it goes through the same parser and StallTracker. Nothing here probes the
network or contacts the Executor: the output says whether a stall would have been confirmed,
not whether a restart would have been allowed.

The collector samples every ~5 s while the agent samples every 2 s. Pass --confirm so that
confirm x collector interval matches the live policy (live 5 x 2 s = 10 s -> replay 2 x 5 s).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import IO, Any

from cra.decide import StallTracker
from cra.observe import Observation, parse_observation
from cra.policy import Policy


def observations(stream: IO[str], *, drop_acked: bool) -> Iterator[Observation]:
    for line in stream:
        try:
            sample = json.loads(line)
        except json.JSONDecodeError:
            continue
        source = sample.get("source")
        if sample.get("source_status") != "VALID" or not isinstance(source, dict) or not source.get("tcp_metrics"):
            continue
        raw = dict(source)
        raw["ffmpeg_generation"] = source.get("source_ffmpeg_generation") or source.get("ffmpeg_generation")
        raw["pod_uid"] = (source.get("runtime_identity") or {}).get("pod_uid") or source.get("pod_uid")
        raw["executor_instance_id"] = source.get("producer_instance_id") or source.get("executor_instance_id")
        if drop_acked:
            raw["tcp_metrics"] = {k: v for k, v in source["tcp_metrics"].items() if k != "bytes_acked"}
        value, _ = parse_observation(raw, max_age_sec=None)
        if value is not None:
            yield value


@dataclass
class Detection:
    criterion: str
    detected_at: datetime
    generation_key: str
    uptime_sec: int
    started_at: datetime
    outcome: str = "OPEN"
    outcome_at: datetime | None = None

    def close(self, outcome: str, at: datetime) -> None:
        self.outcome, self.outcome_at = outcome, at

    def to_dict(self) -> dict[str, Any]:
        return {
            "criterion": self.criterion,
            "stall_started_at": self.started_at.isoformat(),
            "confirmed_at": self.detected_at.isoformat(),
            "generation_key": self.generation_key,
            "uptime_sec": self.uptime_sec,
            "outcome": self.outcome,
            "outcome_after_sec": None if self.outcome_at is None else round((self.outcome_at - self.detected_at).total_seconds(), 1),
        }


@dataclass
class Replay:
    criterion: str
    policy: Policy
    max_gap_sec: float
    window_sec: float
    tracker: StallTracker = field(default_factory=StallTracker)
    open: Detection | None = None
    first_stalled_at: datetime | None = None
    detections: list[Detection] = field(default_factory=list)
    candidates: list[Detection] = field(default_factory=list)  # still stalled after the grace window

    def step(self, obs: Observation) -> None:
        if self.open and obs.generation_key != self.open.generation_key:
            self.open.close("FFMPEG_REPLACED", obs.observed_at)
            self.open = None
        stalled, streak = self.tracker.update(obs, self.policy, max_gap_sec=self.max_gap_sec)
        if streak == 1:
            self.first_stalled_at = obs.observed_at
        if self.open and streak == 0:
            self.open.close("SELF_RECOVERED", obs.observed_at)
            self.open = None
        if self.open and (obs.observed_at - self.open.detected_at).total_seconds() > self.window_sec:
            self.open.close("STILL_STALLED", obs.observed_at)
            self.open = None
        if streak == self.policy.stall_confirm_samples and stalled:
            self.open = Detection(
                self.criterion,
                obs.observed_at,
                obs.generation_key,
                obs.ffmpeg_uptime_sec,
                self.first_stalled_at or obs.observed_at,
            )
            self.detections.append(self.open)
        started = self.tracker.stall_started_at
        if (
            self.open
            and stalled
            and started is not None
            and (obs.observed_at - started).total_seconds() >= self.policy.stall_grace_sec
            and (not self.candidates or self.candidates[-1] is not self.open)
        ):
            self.candidates.append(self.open)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="replay_tcp_samples")
    parser.add_argument("files", nargs="*", help="collector samples.jsonl files; stdin when omitted")
    parser.add_argument("--confirm", type=int, default=2)
    parser.add_argument("--grace-sec", type=int, default=90, help="stall_grace_sec: when a confirmed stall becomes a candidate")
    parser.add_argument("--max-gap-sec", type=float, default=8.0)
    parser.add_argument("--window-sec", type=float, default=120.0)
    parser.add_argument("--lookback-sec", type=float, default=120.0, help="how far before a replacement a detection counts")
    args = parser.parse_args(argv)
    policy = Policy(stall_confirm_samples=args.confirm, stall_grace_sec=args.grace_sec)

    replays = {name: Replay(name, policy, args.max_gap_sec, args.window_sec) for name in ("ack", "bytes_sent_only")}
    replacements: list[tuple[datetime, str, str]] = []
    counts: Counter[str] = Counter()
    last: Observation | None = None

    def feed(stream: IO[str]) -> None:
        nonlocal last
        for obs in observations(stream, drop_acked=False):
            counts["samples"] += 1
            if obs.tcp is not None and obs.tcp.bytes_acked is None:
                counts["samples_without_bytes_acked"] += 1
            if last is not None and obs.generation_key != last.generation_key and obs.pod_uid == last.pod_uid:
                replacements.append((obs.observed_at, last.generation_key, obs.generation_key))
            last = obs
            replays["ack"].step(obs)
            has_acked = obs.tcp is not None and obs.tcp.bytes_acked is not None
            replays["bytes_sent_only"].step(_without_acked(obs) if has_acked else obs)

    if args.files:
        for path in args.files:
            with open(path, encoding="utf-8") as stream:
                feed(stream)
    else:
        feed(sys.stdin)
    for replay in replays.values():
        if replay.open:
            replay.open.close("END_OF_DATA", last.observed_at if last else replay.open.detected_at)

    for replay in replays.values():
        for detection in replay.detections:
            print(
                json.dumps({"type": "detection", **detection.to_dict(), "grace_candidate": any(c is detection for c in replay.candidates)})
            )
    matched = Counter()
    for at, before, after in replacements:
        hit = {
            name: any(
                d.generation_key == before and 0 <= (at - d.detected_at).total_seconds() <= args.lookback_sec for d in replay.detections
            )
            for name, replay in replays.items()
        }
        for name, value in hit.items():
            matched[name] += int(value)
        print(json.dumps({"type": "replacement", "at": at.isoformat(), "from": before, "to": after, "detected_before": hit}))
    summary = {
        "type": "summary",
        "samples": counts["samples"],
        "samples_without_bytes_acked": counts["samples_without_bytes_acked"],
        "confirm_samples": args.confirm,
        "grace_sec": args.grace_sec,
        "ffmpeg_replacements": len(replacements),
        "criteria": {
            name: {
                "detections": len(replay.detections),
                "outcomes": dict(Counter(d.outcome for d in replay.detections)),
                "replacements_detected_before": matched[name],
                "detected_below_min_uptime": sum(d.uptime_sec < policy.min_ffmpeg_uptime_sec for d in replay.detections),
                "grace_candidates": len(replay.candidates),
                "grace_candidate_outcomes": dict(Counter(d.outcome for d in replay.candidates)),
            }
            for name, replay in replays.items()
        },
    }
    print(json.dumps(summary))
    return 0


def _without_acked(obs: Observation) -> Observation:
    raw = dict(obs.raw)
    raw["tcp_metrics"] = {k: v for k, v in (raw.get("tcp_metrics") or {}).items() if k != "bytes_acked"}
    value, _ = parse_observation(raw, max_age_sec=None)
    assert value is not None
    return value


if __name__ == "__main__":
    sys.exit(main())
