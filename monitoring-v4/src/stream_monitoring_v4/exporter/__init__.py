"""Read-only Prometheus views over the isolated Monitoring v4 database."""

from .metrics import render_metrics

__all__ = ["render_metrics"]
