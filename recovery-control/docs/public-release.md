# Public Release Boundary

This source tree presents the current local reflex CRA: 17 Python files and
2,535 lines under `src/cra/`, plus its tests, tools, examples and English docs.
The original public snapshot and later selected ports remain traceable through
the top-level publication records and Git history.

On 2026-10-26, the public layout was narrowed to this implementation. Required
Stream Executor and observation modules moved unchanged to the top-level Stream
source. The previous central authority, fallback agent, Monitoring projection,
soak machinery and general harness remain at the exact [historical revision](history.md).
The [layout record](../../publication/cra-layout-20261026.json) identifies moved
files and the original revision. Dated evidence and failed attempts remain intact.

Only `cra` and `cra.central` are included in the CRA distribution. Its runtime
dependency is `cryptography`. The test extra supplies the dependencies for real
Executor integration checks using the companion Stream source; no fixed SQLite
build, PostgreSQL client or private immutable release is required.

Configuration examples keep `dry_run`; service examples are inert until an
operator installs them under a separately approved deployment. This reorganization
did not deploy code, change policy or authority, restart services, or alter live
state. Local tests are not provider, receiver, recovery-benefit or formal-soak
proof. Private keys, raw logs, runtime databases, addresses and host workflows
remain excluded.
