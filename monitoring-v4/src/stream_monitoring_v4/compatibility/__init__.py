"""Read-only comparison and conversion helpers for the stream_v3 migration."""

from .current_diff import CurrentDiffReport, compare_current_projection

__all__ = ["CurrentDiffReport", "compare_current_projection"]
