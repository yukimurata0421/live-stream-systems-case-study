"""Dell reflex loop: observe -> decide -> reserve -> request -> verify. Runs standalone; CRA is optional."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import signal
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra import effect, signing
from cra.decide import NONE, RESTART, WOULD_RESTART, Decision, StallTracker, decide
from cra.observe import Network, Observation, Prober, read_observation
from cra.policy import BUILTIN, Policy
from cra.store import AgentStore
from cra.sync import CentralSync, load_cached_policy

HEARTBEAT_SEC = 600
RELEASE_FILE = Path(__file__).resolve().parents[2] / "RELEASE"


def release_id() -> str:
    try:
        return RELEASE_FILE.read_text().strip()
    except OSError:
        return "unreleased"


@dataclass(frozen=True)
class CentralConfig:
    url: str
    policy_public_key: str
    agent_key_path: Path
    sync_interval_sec: float = 30.0


@dataclass(frozen=True)
class Config:
    agent_id: str
    db_path: Path
    observation_path: Path
    executor_socket: Path
    producer_id: str
    producer_generation: int
    stop_file: Path
    status_path: Path
    ingest_host: str
    ingest_port: int
    public_hosts: tuple[str, ...] = ("1.1.1.1", "8.8.8.8")
    interval_sec: float = 2.0
    observation_max_age_sec: float = 3.0
    central: CentralConfig | None = None

    @classmethod
    def load(cls, path: Path) -> Config:
        raw = json.loads(path.read_text())
        central = raw.pop("central", None)
        for name in ("db_path", "observation_path", "executor_socket", "stop_file", "status_path"):
            raw[name] = Path(raw[name])
        raw["public_hosts"] = tuple(raw.get("public_hosts", cls.public_hosts))
        if central:
            central["agent_key_path"] = Path(central["agent_key_path"])
            raw["central"] = CentralConfig(**central)
        return cls(**raw)


@dataclass
class Counters:
    samples: int = 0
    unavailable: dict[str, int] = field(default_factory=dict)
    stalled_samples: int = 0
    confirmed: int = 0

    def snapshot_and_reset(self) -> dict[str, Any]:
        value = {
            "samples": self.samples,
            "unavailable": dict(self.unavailable),
            "stalled_samples": self.stalled_samples,
            "confirmed_stall_samples": self.confirmed,
        }
        self.samples, self.unavailable, self.stalled_samples, self.confirmed = 0, {}, 0, 0
        return value


def _now() -> datetime:
    return datetime.now(UTC)


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(value, sort_keys=True, indent=1, default=str))
    os.replace(tmp, path)


class Agent:
    def __init__(
        self,
        config: Config,
        *,
        store: AgentStore | None = None,
        prober: Prober | None = None,
        sender: Callable[[Path, dict[str, Any]], dict[str, Any]] = effect.send,
        clock: Callable[[], datetime] = _now,
        log: Callable[[str], None] = lambda line: print(line, flush=True),
        write_status: bool = True,
    ) -> None:
        self.config = config
        self.store = store or AgentStore(config.db_path)
        self.prober = prober or Prober(list(config.public_hosts), config.ingest_host, config.ingest_port)
        self.sender = sender
        self.clock = clock
        self.log = log
        self.tracker = StallTracker()
        self.counters = Counters()
        self.last_summary = ""
        self.last_heartbeat = clock()
        self.last_sync_status = ""
        self.status: dict[str, Any] = {}
        self.write_status = write_status
        self._close_interrupted_actions()

    # --- policy ------------------------------------------------------------------------

    def policy(self) -> Policy:
        central = self.config.central
        if central is None:
            return BUILTIN
        return load_cached_policy(self.store, central.policy_public_key) or BUILTIN

    # --- one cycle ---------------------------------------------------------------------

    def step(self) -> Decision:
        now = self.clock()
        policy = self.policy()
        stop = self.config.stop_file.exists()
        self.counters.samples += 1
        obs, why = read_observation(self.config.observation_path, max_age_sec=self.config.observation_max_age_sec, now=now)
        if obs is None:
            self.counters.unavailable[why] = self.counters.unavailable.get(why, 0) + 1
            decision = Decision(NONE, why)
            self._record_decision(now, decision, policy, obs=None, streak=0, network=None)
            self._finish(now, policy, decision, None, 0, stop)
            return decision

        stalled, streak = self.tracker.update(obs, policy)
        self.counters.stalled_samples += int(stalled)
        network = self.prober.probe() if streak >= policy.stall_confirm_samples else None
        if network is not None:
            self.counters.confirmed += 1
        mode = "dry_run" if policy.mode != "enforce" else "enforce"
        decision = decide(
            now=now,
            obs=obs,
            stall_streak=streak,
            network=network,
            history=self.store.history(now=now, generation_key=obs.generation_key, mode=mode),
            policy=policy,
            producer_id=self.config.producer_id,
            producer_generation=self.config.producer_generation,
            stop_file_present=stop,
        )
        self._record_decision(now, decision, policy, obs=obs, streak=streak, network=network)
        if decision.outcome in (RESTART, WOULD_RESTART):
            self._act(now, decision, policy, obs, streak, network)
        self._verify(now, policy, obs)
        self._finish(now, policy, decision, obs, streak, stop)
        return decision

    def _evidence(self, obs: Observation | None, streak: int, network: Network | None) -> dict[str, Any]:
        if obs is None:
            return {}
        return {
            "generation_key": obs.generation_key,
            "observation_id": obs.observation_id,
            "sequence": obs.sequence,
            "uptime_sec": obs.ffmpeg_uptime_sec,
            "streak": streak,
            "tcp": vars(obs.tcp) if obs.tcp else None,
            "network": None if network is None else {**vars(network), "network_down": network.network_down},
        }

    def _record_decision(
        self, now: datetime, decision: Decision, policy: Policy, *, obs: Observation | None, streak: int, network: Network | None
    ) -> None:
        if decision.summary == self.last_summary:
            return
        self.last_summary = decision.summary
        payload = {
            "outcome": decision.outcome,
            "reason": decision.reason,
            "blockers": list(decision.blockers),
            "policy_version": policy.version,
            "mode": policy.mode,
            **self._evidence(obs, streak, network),
        }
        self.store.audit(now, "decision", payload)
        self.log(f"decision {decision.summary} gen={payload.get('generation_key', '-')} streak={streak}")

    def _act(self, now: datetime, decision: Decision, policy: Policy, obs: Observation, streak: int, network: Network | None) -> None:
        mode = "enforce" if decision.outcome == RESTART else "dry_run"
        request_id = effect.request_id_for(obs.generation_key)
        action_id = self.store.reserve_action(
            now=now,
            generation_key=obs.generation_key,
            mode=mode,
            request_id=request_id,
            policy_version=policy.version,
            bytes_sent=obs.tcp.bytes_sent if obs.tcp else 0,
            evidence=self._evidence(obs, streak, network),
        )
        if action_id is None:
            return  # generation already claimed (I1)
        if mode == "dry_run":
            self.store.complete_action(now, action_id, "DRY_RUN", {})
            self.log(f"would restart gen={obs.generation_key} (dry_run)")
            return
        request = effect.build_restart_request(
            obs,
            now=now,
            producer_id=self.config.producer_id,
            producer_generation=self.config.producer_generation,
            reason=f"cra: confirmed tcp stall streak={streak}",
        )
        try:
            response = self.sender(self.config.executor_socket, request)
            state = effect.classify(response)
        except Exception as exc:  # noqa: BLE001 - any transport failure leaves the outcome unknown (I3)
            response, state = {"error": f"{type(exc).__name__}: {exc}"[:300]}, "OUTCOME_UNKNOWN"
        self.store.complete_action(self.clock(), action_id, state, response)
        self.log(f"restart gen={obs.generation_key} -> {state}")

    def _verify(self, now: datetime, policy: Policy, obs: Observation) -> None:
        for row in self.store.unverified_actions():
            age = (now - datetime.fromisoformat(row["created_at"])).total_seconds()
            same_generation = obs.generation_key == row["generation_key"]
            tcp = obs.tcp
            flowing = tcp is not None and tcp.bytes_sent > 0 and tcp.lastsnd_ms < policy.stall_lastsnd_ms
            result = ""
            if row["state"] in ("REJECTED", "EFFECT_FAILED"):
                result = "NOT_EXECUTED"
            elif row["mode"] == "enforce":
                if not same_generation and obs.ffmpeg_running and flowing:
                    result = "RECOVERED"
                elif age > policy.verify_window_sec:
                    result = "NOT_RECOVERED"
            else:
                if same_generation and tcp is not None and tcp.bytes_sent > row["bytes_sent_at_action"]:
                    result = "SELF_RECOVERED"
                elif not same_generation:
                    result = "GENERATION_CHANGED_WITHOUT_ACTION"
                elif age > policy.verify_window_sec:
                    result = "STILL_STALLED"
            if result:
                self.store.record_verification(
                    now, row["id"], result, {"after_sec": round(age, 1), "observed_generation_key": obs.generation_key}
                )
                self.log(f"verified action={row['id']} {result}")

    def _close_interrupted_actions(self) -> None:
        """An action left RESERVED means the agent died around the request: its outcome is unknown (I3)."""
        for row in self.store.unverified_actions():
            if row["state"] == "RESERVED":
                self.store.complete_action(self.clock(), row["id"], "OUTCOME_UNKNOWN", {"error": "AGENT_RESTARTED"})

    def _finish(self, now: datetime, policy: Policy, decision: Decision, obs: Observation | None, streak: int, stop: bool) -> None:
        if (now - self.last_heartbeat).total_seconds() >= HEARTBEAT_SEC:
            self.last_heartbeat = now
            self.store.audit(
                now, "heartbeat", {"policy_version": policy.version, "mode": policy.mode, **self.counters.snapshot_and_reset()}
            )
        self.status = {
            "agent_id": self.config.agent_id,
            "release": release_id(),
            "updated_at": now.isoformat(),
            "policy": {"version": policy.version, "mode": policy.mode},
            "local_stop": stop,
            "decision": {"outcome": decision.outcome, "reason": decision.reason, "blockers": list(decision.blockers)},
            "generation_key": obs.generation_key if obs else None,
            "stall_streak": streak,
            "audit_last_seq": self.store.last_seq(),
            "central_sync": self.last_sync_status,
        }
        if self.write_status:
            with contextlib.suppress(OSError):
                _write_json_atomic(self.config.status_path, self.status)


def _sync_loop(config: Config, stop: threading.Event, agent: Agent) -> None:
    central = config.central
    assert central is not None
    store = AgentStore(config.db_path)  # own connection: sqlite objects are per-thread
    sync = CentralSync(
        store=store,
        agent_id=config.agent_id,
        url=central.url,
        policy_public_key=central.policy_public_key,
        agent_key=signing.load_private_key(central.agent_key_path),
    )
    while not stop.is_set():
        agent.last_sync_status = sync.cycle(_now())
        stop.wait(central.sync_interval_sec)
    store.close()


def run(config: Config) -> int:
    agent = Agent(config)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    agent.store.audit(_now(), "agent_start", {"agent_id": config.agent_id, "pid": os.getpid(), "release": release_id()})
    agent.log(f"cra-agent {release_id()} started")
    if config.central is not None:
        threading.Thread(target=_sync_loop, args=(config, stop, agent), name="cra-sync", daemon=True).start()
    while not stop.is_set():
        try:
            agent.step()
        except Exception as exc:  # noqa: BLE001 - keep the reflex loop alive; failures are logged
            agent.log(f"step error {type(exc).__name__}: {exc}")
        stop.wait(config.interval_sec)
    agent.store.audit(_now(), "agent_stop", {"agent_id": config.agent_id})
    return 0


class _NoSend:
    def __call__(self, *_: Any) -> dict[str, Any]:
        raise RuntimeError("once mode never sends requests")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cra-agent")
    parser.add_argument("command", choices=("run", "once", "status"))
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    config = Config.load(args.config)
    if args.command == "run":
        return run(config)
    if args.command == "status":
        print(config.status_path.read_text() if config.status_path.exists() else "{}")
        return 0
    # once: read inputs and evaluate with a throwaway store; nothing is reserved, sent or recorded.
    # A single sample cannot confirm a stall, so this checks input health and the active policy.
    agent = Agent(config, store=AgentStore(Path(":memory:")), sender=_NoSend(), log=lambda _: None, write_status=False)
    if config.central is not None and config.db_path.exists():
        cached = load_cached_policy(AgentStore(config.db_path), config.central.policy_public_key)
        agent.policy = lambda: cached or BUILTIN  # type: ignore[method-assign]
    decision = agent.step()
    print(json.dumps({"decision": decision.summary, **agent.status}, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
