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
from cra.decide import NONE, RESTART, WOULD_RESTART, Decision, History, StallTracker, decide
from cra.observe import Endpoint, Network, Observation, Prober, read_observation
from cra.policy import BUILTIN, Policy
from cra.recovery import after_request, delivery_sample, sending_progress
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
    ingest_endpoints: dict[str, Endpoint] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> Config:
        raw = json.loads(path.read_text())
        central = raw.pop("central", None)
        for name in ("db_path", "observation_path", "executor_socket", "stop_file", "status_path"):
            raw[name] = Path(raw[name])
        raw["public_hosts"] = tuple(raw.get("public_hosts", cls.public_hosts))
        raw["ingest_endpoints"] = {route: Endpoint(**value) for route, value in raw.get("ingest_endpoints", {}).items()}
        if central:
            central["agent_key_path"] = Path(central["agent_key_path"])
            raw["central"] = CentralConfig(**central)
        return cls(**raw)

    def endpoint(self, obs: Observation) -> Endpoint | None:
        route = obs.raw.get("delivery_route")
        if "delivery_transport" in obs.raw:
            binding = obs.binding
            if binding is None:
                return None
            endpoint = Endpoint(binding["peer_host"], binding["peer_port"])
            return endpoint if self.ingest_endpoints.get(str(route)) == endpoint else None
        if route is not None:
            return self.ingest_endpoints.get(str(route))
        # Legacy publishers without route information may use the single fixed target.
        return Endpoint(self.ingest_host, self.ingest_port) if not self.ingest_endpoints else None


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
        cancelled: Callable[[], bool] = lambda: False,
        status_reader: Callable[[Path, str], dict[str, Any]] = effect.read_status,
        unresolved_reader: Callable[[Path], list[dict[str, Any]]] = effect.query_unresolved,
    ) -> None:
        self.config = config
        self.store = store or AgentStore(config.db_path)
        self.prober = prober or Prober(list(config.public_hosts))
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
        self.cancelled = cancelled
        self.status_reader = status_reader
        self.recovery_samples: dict[int, dict[str, Any]] = {}
        self.delivery_progress: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
        self.unresolved_reader = unresolved_reader
        self.last_reconcile_attempt: datetime | None = None
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
            self.tracker.reset()
            self.recovery_samples.clear()
            self.counters.unavailable[why] = self.counters.unavailable.get(why, 0) + 1
            decision = Decision(NONE, why)
            self._record_decision(now, decision, policy, obs=None, streak=0, network=None)
            self._finish(now, policy, decision, None, 0, stop)
            return decision

        stalled, streak = self.tracker.update(obs, policy, max_gap_sec=self.config.interval_sec + self.config.observation_max_age_sec)
        self.counters.stalled_samples += int(stalled)
        endpoint = self.config.endpoint(obs)
        network = self.prober.probe(endpoint) if endpoint is not None and streak >= policy.stall_confirm_samples else None
        network_checked_at = self.clock()
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
        if endpoint is None and obs.ffmpeg_running and obs.ffmpeg_generation:
            decision = Decision(NONE, "TARGET_MISMATCH", ("INGEST_ENDPOINT_UNMAPPED",))
        if decision.outcome in (RESTART, WOULD_RESTART):
            decision = self._act(decision, policy, obs, streak, network, network_checked_at)
        now = self.clock()
        # Verification also reads after the potentially blocking probe/request.
        latest, why = read_observation(self.config.observation_path, max_age_sec=self.config.observation_max_age_sec, now=now)
        if latest is not None:
            self._verify(now, policy, latest)
            if policy.mode == "enforce" and latest.in_flight_count > 0:
                self._reconcile_executor(now, latest)
        else:
            self.tracker.reset()
            self.recovery_samples.clear()
            self.counters.unavailable[why] = self.counters.unavailable.get(why, 0) + 1
            if decision.outcome == NONE:
                decision = Decision(NONE, why)
        self._record_decision(now, decision, policy, obs=obs, streak=streak, network=network)
        self._finish(now, self.policy(), decision, latest, self.tracker.streak, self.config.stop_file.exists())
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
            "executor_instance_id": obs.executor_instance_id,
            "delivery_route": obs.raw.get("delivery_route"),
            "transport_binding_status": obs.binding_status,
            "transport_binding": obs.binding,
            "ingest_endpoint": vars(self.config.endpoint(obs)) if self.config.endpoint(obs) else None,
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

    def _dispatch_guard(
        self,
        policy: Policy,
        original: Observation,
        streak: int,
        network: Network | None,
        network_checked_at: datetime,
        *,
        reserved: bool = False,
    ) -> tuple[datetime, Observation | None, Decision]:
        """Revalidate both before reservation and after its durable commit, immediately before send."""
        current_policy = self.policy()
        now = self.clock()
        if self.cancelled() or self.config.stop_file.exists():
            return now, None, Decision(NONE, "DISPATCH_BLOCKED", ("LOCAL_STOP",))
        if current_policy != policy:
            return now, None, Decision(NONE, "DISPATCH_BLOCKED", ("POLICY_CHANGED",))
        if not 0 <= (now - network_checked_at).total_seconds() <= self.config.observation_max_age_sec:
            return now, None, Decision(NONE, "DISPATCH_BLOCKED", ("NETWORK_EVIDENCE_STALE",))
        obs, why = read_observation(self.config.observation_path, max_age_sec=self.config.observation_max_age_sec, now=now)
        if obs is None:
            self.tracker.reset()
            return now, None, Decision(NONE, "DISPATCH_BLOCKED", (why,))
        if (
            obs.transport_identity != original.transport_identity
            or obs.raw.get("target_identity") != original.raw.get("target_identity")
            or self.config.endpoint(obs) != self.config.endpoint(original)
        ):
            self.tracker.reset()
            return now, obs, Decision(NONE, "DISPATCH_BLOCKED", ("TARGET_CHANGED",))
        if obs.sequence < original.sequence or obs.observed_at < original.observed_at:
            self.tracker.reset()
            return now, obs, Decision(NONE, "DISPATCH_BLOCKED", ("OBSERVATION_REGRESSED",))
        _, current_streak = self.tracker.update(obs, policy, max_gap_sec=self.config.interval_sec + self.config.observation_max_age_sec)
        # A newly read healthy sample invalidates the earlier decision; never count it twice.
        mode = "enforce" if policy.mode == "enforce" else "dry_run"
        history = History(False, None, 0, 0) if reserved else self.store.history(now=now, generation_key=obs.generation_key, mode=mode)
        result = decide(
            now=now,
            obs=obs,
            stall_streak=min(streak, current_streak),
            network=network,
            history=history,
            policy=policy,
            producer_id=self.config.producer_id,
            producer_generation=self.config.producer_generation,
            stop_file_present=False,
        )
        if self.cancelled() or self.config.stop_file.exists():
            return now, obs, Decision(NONE, "DISPATCH_BLOCKED", ("LOCAL_STOP",))
        return now, obs, result

    def _act(
        self,
        decision: Decision,
        policy: Policy,
        obs: Observation,
        streak: int,
        network: Network | None,
        network_checked_at: datetime,
    ) -> Decision:
        now, latest, guarded = self._dispatch_guard(policy, obs, streak, network, network_checked_at)
        if guarded.outcome != decision.outcome or latest is None:
            return guarded
        obs = latest
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
            return Decision(NONE, "STALL_CONFIRMED", ("GENERATION_ALREADY_ACTED",))
        now, latest, guarded = self._dispatch_guard(policy, obs, streak, network, network_checked_at, reserved=True)
        if guarded.outcome != decision.outcome or latest is None:
            self.store.complete_action(now, action_id, "CANCELLED_BEFORE_SEND", {"reason": guarded.summary})
            return guarded  # Preserve the reservation and budget consumption (D2).
        obs = latest
        if mode == "dry_run":
            self.store.complete_action(now, action_id, "DRY_RUN", {})
            self.log(f"would restart gen={obs.generation_key} (dry_run)")
            return decision
        # No network probe or database operation between this fresh timestamp and send.
        issued_at = self.clock()
        request = effect.build_restart_request(
            obs,
            now=issued_at,
            producer_id=self.config.producer_id,
            producer_generation=self.config.producer_generation,
            reason=f"cra: confirmed tcp stall streak={streak}",
        )
        try:
            response = self.sender(self.config.executor_socket, request)
            state = effect.classify(response, expected_request_id=request["request_id"])
        except Exception as exc:  # noqa: BLE001 - any transport failure leaves the outcome unknown (I3)
            response, state = {"error": f"{type(exc).__name__}: {exc}"[:300]}, "OUTCOME_UNKNOWN"
        response = {
            **response,
            "agent_request_issued_at": issued_at.isoformat(),
            "agent_executor_instance_id": obs.executor_instance_id,
            "agent_pod_uid": obs.pod_uid,
            "agent_verify_window_sec": policy.verify_window_sec,
            "agent_previous_generation": obs.ffmpeg_generation,
            "agent_delivery_route": obs.raw.get("delivery_route"),
            "agent_delivery_route_generation": obs.raw.get("delivery_route_generation"),
            "agent_max_sample_gap_sec": self.config.interval_sec + self.config.observation_max_age_sec,
            "agent_stall_lastsnd_ms": policy.stall_lastsnd_ms,
        }
        self.store.complete_action(self.clock(), action_id, state, response)
        self.log(f"restart gen={obs.generation_key} -> {state}")
        return decision

    def _verify(self, now: datetime, policy: Policy, obs: Observation) -> None:
        queried = False
        for stored in self.store.unverified_actions():
            row = dict(stored)
            response = json.loads(row["response"] or "{}")
            if (
                row["mode"] == "enforce"
                and row["state"] == "OUTCOME_UNKNOWN"
                and not queried
                and response.get("agent_executor_instance_id") == obs.executor_instance_id
                and response.get("agent_pod_uid") == obs.pod_uid
            ):
                queried = True
                try:
                    status = self.status_reader(self.config.executor_socket, row["request_id"])
                except (OSError, ValueError, TypeError):
                    status = {}
                if not isinstance(status, dict):
                    status = {}
                latest, _ = read_observation(
                    self.config.observation_path, max_age_sec=self.config.observation_max_age_sec, now=self.clock()
                )
                # Legacy status has no executor instance field. Bracket the read with fresh,
                # stable observations; a changed target/epoch defers reconciliation and verification.
                if (
                    latest is None
                    or latest.transport_identity != obs.transport_identity
                    or latest.sequence < obs.sequence
                    or latest.observed_at < obs.observed_at
                ):
                    self.recovery_samples.pop(row["id"], None)
                    continue
                obs, now = latest, self.clock()
                if (
                    status.get("schema_version") == "runtime.effect_request_status_response.v1"
                    and status.get("ok") is True
                    and status.get("request_id") == row["request_id"]
                    and ("executor_instance_id" not in status or status["executor_instance_id"] == obs.executor_instance_id)
                    and status.get("state") == "EFFECT_OBSERVED"
                    and status.get("effect_boundary_reached") is True
                    and type(status.get("physical_attempt_count")) is int
                    and status["physical_attempt_count"] == 1
                    and status.get("effect_scope_state")
                    in {"EFFECT_OBSERVED", "EFFECT_OBSERVED_AWAITING_VERIFICATION", "RECONCILED_EFFECT_OBSERVED"}
                    and isinstance(status.get("effect_scope_id"), str)
                    and status["effect_scope_id"]
                ):
                    response["executor_reconciliation"] = status
                    self.store.reconcile_observed_action(self.clock(), row["id"], response)
                    row.update(state="EFFECT_OBSERVED", response=json.dumps(response))
            age = (now - datetime.fromisoformat(row["created_at"])).total_seconds()
            same_generation = obs.generation_key == row["generation_key"]
            tcp = obs.tcp
            sample = delivery_sample(obs)
            progress = self.delivery_progress.get(row["id"])
            if progress and any(
                progress[1].get(k) != sample[k]
                for k in ("generation_key", "executor_instance_id", "delivery_route", "delivery_route_generation")
            ):
                self.delivery_progress.pop(row["id"], None)
                progress = None
            result = ""
            if row["state"] in ("REJECTED", "EFFECT_FAILED", "CANCELLED_BEFORE_SEND"):
                result = "NOT_EXECUTED"
            elif row["mode"] == "enforce":
                response = json.loads(row["response"] or "{}")
                issued = response.get("agent_request_issued_at")
                action_age = (now - datetime.fromisoformat(issued)).total_seconds() if issued else age
                window = response.get("agent_verify_window_sec", policy.verify_window_sec)
                if not same_generation and obs.ffmpeg_running and obs.ffmpeg_generation and obs.target_valid:
                    same_runtime = (
                        response.get("agent_executor_instance_id") == obs.executor_instance_id
                        and response.get("agent_pod_uid") == obs.pod_uid
                        and response.get("agent_delivery_route") == sample["delivery_route"]
                        and response.get("agent_delivery_route_generation") == sample["delivery_route_generation"]
                    )
                    if not same_runtime or (action_age <= window and not after_request(sample, response)):
                        result = "GENERATION_CHANGED_UNATTRIBUTED"
                    else:
                        previous = self.recovery_samples.get(row["id"], {})
                        if (
                            after_request(sample, response)
                            and after_request(previous, response)
                            and sending_progress(
                                previous,
                                sample,
                                max_gap_sec=self.config.interval_sec + self.config.observation_max_age_sec,
                                lastsnd_ms=response.get("agent_stall_lastsnd_ms", policy.stall_lastsnd_ms),
                            )
                        ):
                            progress = (previous, sample)
                            self.delivery_progress[row["id"]] = progress
                        self.recovery_samples[row["id"]] = sample
                        if progress and row["state"] == "EFFECT_OBSERVED":
                            result = "DELIVERY_RESUMED"
                        elif action_age > window:
                            result = (
                                "DELIVERY_RESUMED_EXECUTION_UNKNOWN"
                                if progress
                                else ("OUTCOME_UNRESOLVED" if row["state"] == "OUTCOME_UNKNOWN" else "NOT_RECOVERED")
                            )
                elif action_age > window:
                    result = "OUTCOME_UNRESOLVED" if row["state"] == "OUTCOME_UNKNOWN" else "NOT_RECOVERED"
            else:
                if same_generation and tcp is not None and tcp.bytes_sent > row["bytes_sent_at_action"]:
                    result = "SELF_RECOVERED"
                elif not same_generation and obs.ffmpeg_running and obs.ffmpeg_generation:
                    result = "GENERATION_CHANGED_WITHOUT_ACTION"
                elif age > policy.verify_window_sec:
                    result = "STILL_STALLED"
            if result:
                self.store.record_verification(
                    now,
                    row["id"],
                    result,
                    {
                        "after_sec": round(age, 1),
                        "observed_generation_key": obs.generation_key,
                        "execution_state": row["state"],
                        "sample": progress[1] if progress else sample,
                        "progress_from": progress[0] if progress else None,
                        "executor_instance_id": obs.executor_instance_id,
                        "observed_at": obs.observed_at.isoformat(),
                        "uptime_sec": obs.ffmpeg_uptime_sec,
                        "tcp": vars(tcp) if tcp else None,
                    },
                )
                self.recovery_samples.pop(row["id"], None)
                self.delivery_progress.pop(row["id"], None)
                self.log(f"verified action={row['id']} {result}")

    def _reconcile_executor(self, now: datetime, obs: Observation) -> None:
        """Close the Executor fence of the agent's own verified recovery (at most once per 30 s).

        The production Executor keeps a delivery restart in EFFECT_OBSERVED_AWAITING_VERIFICATION
        and admits no further request until it is reconciled. Only DELIVERY_RESUMED actions owned
        by this agent qualify; an unknown outcome stays blocked for an operator.
        """
        if self.last_reconcile_attempt and (now - self.last_reconcile_attempt).total_seconds() < 30:
            return
        pending = {
            row["request_id"]: row["id"]
            for row in self.store.resumed_enforce_actions()
            if self.store.get(f"executor_reconciled:{row['id']}") is None
        }
        if not pending:
            return
        self.last_reconcile_attempt = now
        try:
            scopes = self.unresolved_reader(self.config.executor_socket)
        except (OSError, ValueError, TypeError):
            return
        endpoint = self.config.endpoint(obs)
        for scope in scopes:
            action_id = pending.get(scope.get("owner_request_id"))
            if action_id is None or scope.get("state") != "EFFECT_OBSERVED_AWAITING_VERIFICATION":
                continue
            network = self.prober.probe(endpoint) if endpoint is not None else None
            if network is None or network.network_down or not network.ingest_ok:
                self.store.audit(now, "executor_reconciliation", {"action_id": action_id, "result": "DEFERRED_NETWORK"})
                return
            request = effect.build_reconciliation(scope, obs, now=self.clock())
            try:
                response = self.sender(self.config.executor_socket, request)
            except Exception as exc:  # noqa: BLE001 - retried on a later cycle
                response = {"error": f"{type(exc).__name__}: {exc}"[:300]}
            done = response.get("ok") is True and response.get("state") == "RECONCILED_EFFECT_OBSERVED"
            self.store.put(
                f"executor_reconciled:{action_id}" if done else f"executor_reconcile_last:{action_id}",
                json.dumps(response, sort_keys=True),
                now=now,
                audit_kind="executor_reconciliation",
                audit={"action_id": action_id, "result": "RECONCILED" if done else "FAILED", "response": response},
            )
            self.log(f"executor fence action={action_id} -> {'RECONCILED' if done else 'FAILED'}")

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
            "transport_binding_status": obs.binding_status if obs else "UNAVAILABLE",
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
    stop = threading.Event()
    agent = Agent(config, cancelled=stop.is_set)
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
