TERMINAL_ATTEMPT_STATES = frozenset(
    {"succeeded", "permanent_failed", "uncertain"}
)
ATTEMPT_STATES = frozenset(
    {*TERMINAL_ATTEMPT_STATES, "in_flight", "retryable_failed"}
)
