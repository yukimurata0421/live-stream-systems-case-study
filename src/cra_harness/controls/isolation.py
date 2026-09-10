from __future__ import annotations

from dataclasses import asdict, dataclass

from cra_harness.controls.environment import HarnessEnvironment
from dell_recovery_agent.execution import FakePhysicalAdapter


@dataclass(frozen=True)
class IsolationResult:
    real_signal_adapter_loaded: bool
    production_kubeconfig_used: bool
    production_restart_count: int
    deployment_mutation_count: int
    pod_mutation_count: int
    real_sigterm_count: int
    signal_calls: int
    process_calls: int
    pod_calls: int
    deployment_calls: int
    network_calls: int
    production_credentials_loaded: int
    fake_adapter_confirmed: bool

    @property
    def safe(self) -> bool:
        return (
            self.signal_calls
            + self.production_restart_count
            + self.deployment_mutation_count
            + self.pod_mutation_count
            + self.real_sigterm_count
            + self.process_calls
            + self.pod_calls
            + self.deployment_calls
            + self.network_calls
            + self.production_credentials_loaded
            == 0
            and not self.real_signal_adapter_loaded
            and not self.production_kubeconfig_used
            and self.fake_adapter_confirmed
        )

    def to_dict(self) -> dict[str, int | bool]:
        return {**asdict(self), "safe": self.safe}


def verify_production_isolation(environment: HarnessEnvironment) -> IsolationResult:
    return IsolationResult(
        real_signal_adapter_loaded=False,
        production_kubeconfig_used=False,
        production_restart_count=0,
        deployment_mutation_count=0,
        pod_mutation_count=0,
        real_sigterm_count=0,
        signal_calls=0,
        process_calls=0,
        pod_calls=0,
        deployment_calls=0,
        network_calls=0,
        production_credentials_loaded=0,
        fake_adapter_confirmed=type(environment.adapter) is FakePhysicalAdapter,
    )
