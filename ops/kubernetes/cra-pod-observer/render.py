"""Render the Pod API observer for a pinned image and an explicit delivery node."""
import argparse
import json
import re


def manifest(image, node):
    if not re.fullmatch(r"[a-zA-Z0-9._:/-]+@sha256:[a-f0-9]{64}", image):
        raise ValueError("digest-pinned image required")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", node):
        raise ValueError("node name required")
    name = "cra-pod-api-observer"
    label = {"app.kubernetes.io/name": name}
    probe = {"exec": {"command": ["python3", "/app/pod_api_health.py"]},
             "periodSeconds": 10, "timeoutSeconds": 6, "failureThreshold": 3}
    return {"apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name, "namespace": "stream-v3", "labels": label},
        "spec": {"replicas": 1, "strategy": {"type": "Recreate"}, "selector": {"matchLabels": label},
            "template": {"metadata": {"labels": label}, "spec": {
                "nodeSelector": {"kubernetes.io/hostname": node},
                "serviceAccountName": "cra-phase4-target-observer", "automountServiceAccountToken": False,
                "terminationGracePeriodSeconds": 8,
                "securityContext": {"runAsNonRoot": True, "runAsUser": 992, "runAsGroup": 983,
                                    "fsGroup": 983, "seccompProfile": {"type": "RuntimeDefault"}},
                "containers": [{"name": "observer", "image": image, "imagePullPolicy": "Never",
                    "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                        "capabilities": {"drop": ["ALL"]}},
                    "env": [{"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}],
                    "resources": {"requests": {"cpu": "10m", "memory": "24Mi"},
                                  "limits": {"cpu": "250m", "memory": "96Mi"}},
                    "readinessProbe": probe, "livenessProbe": probe,
                    "volumeMounts": [{"name": "socket", "mountPath": "/run/stream-v3-pod-api-observer"},
                                     {"name": "api", "mountPath": "/var/run/cra-observer", "readOnly": True}]}],
                "volumes": [{"name": "socket", "hostPath": {"path": "/run/stream-v3-pod-api-observer", "type": "Directory"}},
                    {"name": "api", "projected": {"defaultMode": 288, "sources": [
                        {"serviceAccountToken": {"path": "token", "expirationSeconds": 600}},
                        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}
                    ]}}]}}}}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--image", required=True)
    p.add_argument("--node", required=True)
    a = p.parse_args()
    print(json.dumps(manifest(a.image, a.node), indent=2))
