from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

from decision.action_gate import GateContext, decide_action  # type: ignore
from decision.evaluator import evaluate  # type: ignore
from decision.policy import Policy  # type: ignore
from evidence.identity import TargetIdentity  # type: ignore
from evidence.ledger import LedgerSnapshot  # type: ignore
from evidence.sources import EvidenceRecord, SourceKind  # type: ignore


class EvidenceDecisionTests(unittest.TestCase):
    def _policy(self) -> Policy:
        return Policy.from_values(
            data_api_ttl_sec=60,
            oauth_ttl_sec=60,
            watch_ttl_sec=60,
            resolver_ttl_sec=150,
            ingest_ttl_sec=15,
            api_cost_ttl_sec=120,
            budget_release_reconfirm_sec=300,
            min_restart_interval_sec=300,
        )

    def _ev(
        self,
        source: SourceKind,
        verdict: str,
        target: TargetIdentity,
        now: int = 1_000,
        ttl_sec: float = 60.0,
    ) -> EvidenceRecord:
        return EvidenceRecord(
            source=source,
            verdict=verdict,
            target=target,
            observed_at=float(now),
            target_epoch=1,
            restart_epoch=1,
            ttl_sec=ttl_sec,
            raw={},
        )

    def _snap(self, records: list[EvidenceRecord], now: int = 1_000) -> LedgerSnapshot:
        return LedgerSnapshot(
            latest_by_source={ev.source: ev for ev in records},
            recent_records=tuple(records),
            current_target_epoch=1,
            current_restart_epoch=1,
            last_restart_at=0.0,
            canonical_target=TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1),
            taken_at=float(now),
        )

    def _ctx(self, history: tuple[int, ...] = ()) -> GateContext:
        return GateContext(
            fail_count=3,
            max_fails=3,
            enforce_restart=True,
            stream_uptime_sec=600,
            min_restart_uptime_sec=60,
            restart_budget_hourly=3,
            restart_budget_daily=12,
            restart_history_ts=history,
            last_restart_ts=0,
            restart_cooldown_sec=300,
            budget_release_reconfirm_sec=300,
            budget_emergency_override_sec=90,
            active_state_first_ts=0,
            api_cost_degraded=False,
            stream_service="stream.service",
        )

    def test_identity_mismatch_does_not_confirm_ended(self) -> None:
        canonical = TargetIdentity(channel_id="UC", video_id="VID_A", target_epoch=1, restart_epoch=1)
        other = TargetIdentity(channel_id="UC", video_id="VID_B", target_epoch=1, restart_epoch=1)
        snap = self._snap(
            [
                self._ev(SourceKind.DATA_API, "ended", canonical),
                self._ev(SourceKind.OAUTH, "ended", other),
                self._ev(SourceKind.INGEST_LOCAL, "live", canonical),
            ]
        )
        decision = evaluate(snap, self._policy())
        self.assertEqual(decision.state, "inconsistent_remote")

    def test_local_ingest_alive_blocks_restart_even_if_confirmed(self) -> None:
        target = TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1)
        snap = self._snap(
            [
                self._ev(SourceKind.DATA_API, "ended", target),
                self._ev(SourceKind.OAUTH, "ended", target),
                self._ev(SourceKind.INGEST_LOCAL, "live", target),
            ]
        )
        decision = evaluate(snap, self._policy())
        self.assertEqual(decision.state, "remote_ended_confirmed")
        action = decide_action(decision, snap, self._policy(), self._ctx())
        self.assertEqual(action.action, "none")
        self.assertIn("local_ingest_alive_contradiction", action.blocked_by)

    def test_budget_release_requires_reconfirm_window(self) -> None:
        target = TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1)
        now = 100_000
        snap = self._snap(
            [
                self._ev(SourceKind.DATA_API, "ended", target, now),
                self._ev(SourceKind.OAUTH, "ended", target, now),
                self._ev(SourceKind.INGEST_LOCAL, "inactive", target, now),
            ],
            now=now,
        )
        decision = evaluate(snap, self._policy())
        history = (now - 86_410, *tuple(now - 80_000 - i for i in range(11)))
        action = decide_action(decision, snap, self._policy(), self._ctx(history=history))
        self.assertEqual(action.action, "none")
        self.assertIn("budget_just_released_need_reconfirm_daily", action.blocked_by)

    def test_budget_exhausted_allows_sustained_confirmed_remote_end_override(self) -> None:
        target = TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1)
        now = 100_000
        snap = self._snap(
            [
                self._ev(SourceKind.DATA_API, "ended", target, now),
                self._ev(SourceKind.OAUTH, "ended", target, now),
                self._ev(SourceKind.INGEST_LOCAL, "inactive", target, now),
            ],
            now=now,
        )
        decision = evaluate(snap, self._policy())
        history = tuple(now - 60 - i for i in range(12))
        ctx = self._ctx(history=history)
        ctx = GateContext(
            **{
                **ctx.__dict__,
                "active_state_first_ts": now - 91,
            }
        )
        action = decide_action(decision, snap, self._policy(), ctx)
        self.assertEqual(action.action, "restart_stream")

    def test_budget_override_requires_fresh_confirmation_sources(self) -> None:
        policy = Policy.from_values(
            data_api_ttl_sec=180,
            oauth_ttl_sec=180,
            watch_ttl_sec=180,
            resolver_ttl_sec=180,
            ingest_ttl_sec=180,
            api_cost_ttl_sec=180,
            budget_release_reconfirm_sec=300,
            min_restart_interval_sec=300,
        )
        target = TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1)
        now = 100_000
        snap = self._snap(
            [
                self._ev(SourceKind.DATA_API, "ended", target, now - 130, ttl_sec=180.0),
                self._ev(SourceKind.OAUTH, "ended", target, now - 130, ttl_sec=180.0),
            ],
            now=now,
        )
        snap = LedgerSnapshot(
            latest_by_source=snap.latest_by_source,
            recent_records=snap.recent_records,
            current_target_epoch=snap.current_target_epoch,
            current_restart_epoch=snap.current_restart_epoch,
            last_restart_at=snap.last_restart_at,
            canonical_target=snap.canonical_target,
            taken_at=snap.taken_at,
        )
        decision = evaluate(snap, policy)
        self.assertEqual(decision.state, "remote_ended_confirmed")
        history = tuple(now - 60 - i for i in range(12))
        ctx = self._ctx(history=history)
        ctx = GateContext(
            **{
                **ctx.__dict__,
                "active_state_first_ts": now - 180,
            }
        )
        action = decide_action(decision, snap, policy, ctx)
        self.assertEqual(action.action, "none")
        self.assertIn("budget_exhausted", ",".join(action.blocked_by))

    def test_watch_inconclusive_does_not_hide_authoritative_live(self) -> None:
        target = TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1)
        snap = self._snap(
            [
                self._ev(SourceKind.DATA_API, "live", target),
                self._ev(SourceKind.OAUTH, "live", target),
                self._ev(SourceKind.WATCH_PAGE, "unknown", target),
                self._ev(SourceKind.INGEST_LOCAL, "live", target),
            ]
        )
        self.assertEqual(evaluate(snap, self._policy()).state, "available")

    def test_public_live_evidence_is_available_when_authoritative_sources_deferred(self) -> None:
        target = TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1)
        snap = self._snap(
            [
                self._ev(SourceKind.WATCH_PAGE, "live", target),
                self._ev(SourceKind.INGEST_LOCAL, "live", target),
            ]
        )
        decision = evaluate(snap, self._policy())
        self.assertEqual(decision.state, "available")

    def test_public_live_evidence_blocks_authoritative_ended_restart(self) -> None:
        target = TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1)
        snap = self._snap(
            [
                self._ev(SourceKind.DATA_API, "ended", target),
                self._ev(SourceKind.OAUTH, "ended", target),
                self._ev(SourceKind.WATCH_PAGE, "live", target),
                self._ev(SourceKind.INGEST_LOCAL, "live", target),
            ]
        )
        decision = evaluate(snap, self._policy())
        self.assertEqual(decision.state, "inconsistent_remote")
        action = decide_action(decision, snap, self._policy(), self._ctx())
        self.assertEqual(action.action, "resync_resolver")

    def test_remote_unknowns_are_unconfirmed_not_healthy(self) -> None:
        target = TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1)
        snap = self._snap(
            [
                self._ev(SourceKind.OAUTH, "unknown", target),
                self._ev(SourceKind.WATCH_PAGE, "unknown", target),
                self._ev(SourceKind.INGEST_LOCAL, "live", target),
            ]
        )
        decision = evaluate(snap, self._policy())
        self.assertEqual(decision.state, "remote_unconfirmed")
        action = decide_action(decision, snap, self._policy(), self._ctx())
        self.assertEqual(action.action, "none")
        self.assertIn("remote_unconfirmed", action.blocked_by)

    def test_public_not_live_becomes_public_degraded_not_restart(self) -> None:
        target = TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1)
        snap = self._snap(
            [
                self._ev(SourceKind.WATCH_PAGE, "not_live", target),
                self._ev(SourceKind.INGEST_LOCAL, "live", target),
            ]
        )
        decision = evaluate(snap, self._policy())
        self.assertEqual(decision.state, "public_degraded")
        action = decide_action(decision, snap, self._policy(), self._ctx())
        self.assertEqual(action.action, "alert")

    def test_epoch_bump_invalidates_pre_restart_evidence(self) -> None:
        old = TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=1)
        ev = self._ev(SourceKind.DATA_API, "ended", old)
        snap = LedgerSnapshot(
            latest_by_source={SourceKind.DATA_API: ev},
            recent_records=(ev,),
            current_target_epoch=1,
            current_restart_epoch=2,
            last_restart_at=999.0,
            canonical_target=TargetIdentity(channel_id="UC", video_id="VID", target_epoch=1, restart_epoch=2),
            taken_at=1_000.0,
        )
        self.assertEqual(evaluate(snap, self._policy()).state, "telemetry_degraded")


if __name__ == "__main__":
    unittest.main()
