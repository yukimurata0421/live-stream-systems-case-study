"""Bounded clock exchanges for observations only; never adjusts an authority expiry.

Offset is remote minus local. Bounds include arbitrary path asymmetry. Local
elapsed time always comes from CLOCK_BOOTTIME (includes host suspend).
"""
from __future__ import annotations

import hashlib
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path

SCHEMA = 'stream.clock_exchange.v1'
EPSILON = .005  # capture/serialization allowance, seconds
STEP_LIMIT = .050
MAX_EXCHANGE = 12.
MODEL_TTL = 180.
PAIR_DRIFT = .001  # conservative 500 ppm per host during holdover


class ClockRejected(ValueError):
    pass


def mono():
    return time.clock_gettime(time.CLOCK_BOOTTIME)


def capture():
    before = mono()
    wall = time.time()
    after = mono()
    if after - before > EPSILON:
        raise ClockRejected('clock_capture_delayed')
    return {'wall': wall, 'mono': (before + after) / 2}


def identity():
    return {'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'machine_id_sha256': hashlib.sha256(Path('/etc/machine-id').read_bytes()).hexdigest()}


def valid_nonce(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{32}', value) is not None


def envelope(nonce, started, *, host_identity=None):
    if not valid_nonce(nonce):
        raise ClockRejected('clock_nonce_invalid')
    bound = identity() if host_identity is None else host_identity
    return {'schema': SCHEMA, 'nonce': nonce, **bound,
            'received': started, 'sent': capture()}


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


@dataclass(frozen=True)
class Estimate:
    low: float
    high: float
    received_wall: float
    received_mono: float
    boot_id: str
    machine: str
    local_boot: str
    network_rtt: float

    def bounds(self, *, now=None, local_identity=None):
        now = capture() if now is None else now
        elapsed = now['mono'] - self.received_mono
        if not 0 <= elapsed <= MODEL_TTL:
            raise ClockRejected('clock_model_expired')
        if abs(now['wall'] - self.received_wall - elapsed) > STEP_LIMIT:
            raise ClockRejected('clock_local_step')
        if local_identity is not None and local_identity['boot_id'] != self.local_boot:
            raise ClockRejected('clock_local_boot_changed')
        drift = elapsed * PAIR_DRIFT
        return self.low - drift, self.high + drift

    def age_bounds(self, source_stamp, *, now=None):
        if not finite(source_stamp):
            raise ClockRejected('clock_timestamp_invalid')
        now = capture() if now is None else now
        low, high = self.bounds(now=now)
        age = self.received_wall + now['mono'] - self.received_mono - source_stamp
        return age + low, age + high

    def fresh(self, source_stamp, max_age, *, now=None):
        low, high = self.age_bounds(source_stamp, now=now)
        # A possible negative age alone is normal uncertainty. A wholly future
        # event, or even possibly expired evidence, is rejected.
        return high >= -EPSILON and high <= max_age

    def diagnostic(self, *, now=None):
        low, high = self.bounds(now=now)
        return {'offset_seconds': (low + high) / 2,
                'uncertainty_seconds': (high - low) / 2,
                'network_rtt_seconds': self.network_rtt,
                'model_ttl_seconds': MODEL_TTL}


def estimate(value, nonce, sent, received, *, machine, boot_id=None, local_boot=None):
    try:
        if value['schema'] != SCHEMA or not valid_nonce(nonce) or value['nonce'] != nonce:
            raise ClockRejected('clock_nonce_mismatch')
        if (len(machine) != 64 or value['machine_id_sha256'] != machine
                or not isinstance(value['boot_id'], str) or not value['boot_id']
                or (boot_id is not None and value['boot_id'] != boot_id)):
            raise ClockRejected('clock_identity_mismatch')
        a, b = value['received'], value['sent']
        if not all(finite(p[k]) for p in (sent, received, a, b) for k in ('wall', 'mono')):
            raise ClockRejected('clock_invalid')
        local = received['mono'] - sent['mono']
        remote = b['mono'] - a['mono']
        if not 0 <= remote <= local + 2 * EPSILON or not 0 <= local <= MAX_EXCHANGE:
            raise ClockRejected('clock_exchange_delayed')
        if (abs(received['wall'] - sent['wall'] - local) > STEP_LIMIT
                or abs(b['wall'] - a['wall'] - remote) > STEP_LIMIT):
            raise ClockRejected('clock_step_during_exchange')
        low = b['wall'] - received['wall'] - EPSILON
        high = a['wall'] - sent['wall'] + EPSILON
        if low > high + EPSILON:
            raise ClockRejected('clock_interval_invalid')
        return Estimate(low, max(low, high), received['wall'], received['mono'], value['boot_id'],
                        machine, identity()['boot_id'] if local_boot is None else local_boot,
                        max(0., local - remote))
    except (KeyError, TypeError, OverflowError) as exc:
        raise ClockRejected('clock_invalid') from exc


def monotonic_source_age(value, nonce, sent, received, *, machine, boot_id, source_mono, max_age=45):
    """Fresh causal facts even during a wall-clock step; no health/auth promotion.

    The source and responder boottimes belong to the same bound boot. Adding the
    whole client exchange gives a conservative age bound without comparing host
    monotonic epochs. Every retrieval still requires a new nonce.
    """
    try:
        if value['schema']!=SCHEMA or not valid_nonce(nonce) or value['nonce']!=nonce:
            raise ClockRejected('clock_nonce_mismatch')
        if value['machine_id_sha256']!=machine or len(machine)!=64 or value['boot_id']!=boot_id:
            raise ClockRejected('clock_identity_mismatch')
        a,b=value['received']['mono'],value['sent']['mono']
        if not all(finite(v) and v>=0 for v in (a,b,sent['mono'],received['mono'],source_mono)):
            raise ClockRejected('clock_invalid')
        elapsed=received['mono']-sent['mono'];remote=b-a
        if not 0<=elapsed<=MAX_EXCHANGE or not 0<=remote<=elapsed+2*EPSILON:
            raise ClockRejected('clock_exchange_delayed')
        age=b-source_mono
        if age < -EPSILON or age+elapsed>max_age:
            raise ClockRejected('clock_source_expired')
        return max(0.,age)+elapsed
    except (KeyError,TypeError,OverflowError) as exc:
        raise ClockRejected('clock_invalid') from exc


class EvidenceCache:
    """Last known evidence expires monotonically; lookup never refreshes it."""
    def __init__(self, max_age):
        self.max_age = max_age
        self.entry = None

    def put(self, value, model, source_stamp):
        self.entry = (value, model, source_stamp)

    def get(self, *, now=None):
        if self.entry is None:
            return None
        value, model, source_stamp = self.entry
        try:
            return value if model.fresh(source_stamp, self.max_age, now=now) else None
        except ClockRejected:
            return None
