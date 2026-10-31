"""Bounded source timestamp validation for public snapshot collection."""
import math

MAX_AGE_SECONDS = 180
MAX_FUTURE_SECONDS = 1


class PublicTimestampError(ValueError):
    """A public, fixed reason code safe to include in collector diagnostics."""


def validate_generated_at(generated, now, *, max_age_seconds=MAX_AGE_SECONDS):
    if type(generated) not in (int, float) or generated <= 0 or (type(generated) is float and not math.isfinite(generated)):
        raise PublicTimestampError('PUBLIC_TIMESTAMP_INVALID')
    if generated > now + MAX_FUTURE_SECONDS:
        raise PublicTimestampError('PUBLIC_TIMESTAMP_FUTURE')
    if generated < now - max_age_seconds:
        raise PublicTimestampError('PUBLIC_TIMESTAMP_STALE')
