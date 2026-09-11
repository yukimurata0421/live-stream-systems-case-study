# Revision-bound soak checkpoints

`ops/scripts/monitoring_v4_soak_checkpoint.py` evaluates a single declared
Monitoring v4 evidence epoch without modifying the repository or database. It
requires both an immutable 40-hex build revision and a known migration-source
revision. Rows from an older revision pair or from before the epoch do not
extend the elapsed gate.

The evaluator reports 24-hour and 72-hour operational checkpoints, the seven-day
coverage gate, and the fourteen-day parity gate. A missing first cycle remains
`EVIDENCE_PENDING`; a first cycle outside the one-cycle anchor allowance remains
`EPOCH_ANCHOR_GAP`; an incomplete elapsed gate is never converted into a pass.
The output also keeps database mutation, real delivery, and runtime mutation
explicitly disabled.

This public commit was derived from uncommitted private worktree candidates on
top of private commit `8f2c1aa03b8d3805d8a4c1d51a3c432b0e59657f`:

- `ops/scripts/monitoring_v4_soak_checkpoint.py`:
  `e3ae635a63028147e288067f0b9ebea4cf9d05dc2eee8b518557be6d78c1d87a`
- `tests/test_soak_checkpoint.py`:
  `894d098a27bde1366f597fc757af34a51aee7f77648fd6991013e5b1ce1bb8b4`

Those identifiers prove source selection only. They do not claim a live
deployment, an elapsed soak, or permission to change Monitoring v4 services.
