"""Release and deployment boundary compatibility checks."""

from .gate import evaluate_release_deployment_compatibility

__all__ = ["evaluate_release_deployment_compatibility"]
