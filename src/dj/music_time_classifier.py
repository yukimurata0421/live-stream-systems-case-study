from __future__ import annotations

"""Compatibility facade for the staged music time classifier.

The implementation lives under ``music_classifier`` so classification rules,
duration probing, manual overrides, library linking, and report rendering can
evolve independently without growing this public entrypoint again.
"""

try:
    from music_classifier import *  # noqa: F401,F403
except ModuleNotFoundError:
    from dj.music_classifier import *  # noqa: F401,F403
