"""Replace only the existing collector Pod API read with the in-Pod observer."""
import os

from pod_api_observer import NAMESPACE, SELECTOR, SOCKET, exchange


def install():
    # The public collector already uses the shared FIFO-aware argv classifier.
    from dell_recovery_agent import target_snapshot
    original = target_snapshot.AtomicKubernetesTargetSnapshotProducer

    class PodApiProducer(original):
        def _pods(self):
            if self.namespace != NAMESPACE or self.label_selector != SELECTOR or self.kubeconfig is not None:
                raise ValueError("POD_API_COLLECTOR_CONFIG_MISMATCH")
            value = exchange(os.environ.get("CRA_POD_API_SOCKET", SOCKET))
            items = value["pods"].get("items", [])
            if not isinstance(items, list) or len(items) != 1:
                raise ValueError("TARGET_POD_CARDINALITY")
            return dict(items[0])

    target_snapshot.AtomicKubernetesTargetSnapshotProducer = PodApiProducer
    return target_snapshot


if __name__ == "__main__":
    install().main()
