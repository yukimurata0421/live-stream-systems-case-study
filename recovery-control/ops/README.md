# Operations examples

This directory contains production-shaped but inert examples for the three
host roles. Public CI does not install units, reload systemd, connect to a
host, read credentials, create databases, or perform recovery effects.

Do not copy an example into a live host unchanged. Resolve the immutable source
and runtime release, external configuration, service account, host contract,
credential ownership, writable state paths, rollback, and stop conditions
first. Files whose names do not end in `.example` are still source examples in
this public snapshot; their presence is not deployment authorization.
