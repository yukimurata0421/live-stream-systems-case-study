"""Detection-only host sentinel for the Monitoring v4 k3s subsystem."""

from .contracts import REPORT_SCHEMA, REQUIRED_COMPONENT_COUNTS, SENTINEL_SCHEMA

__all__ = ["REPORT_SCHEMA", "REQUIRED_COMPONENT_COUNTS", "SENTINEL_SCHEMA"]
