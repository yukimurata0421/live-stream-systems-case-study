# Observability Source Reconciliation

This port closes selected implementation gaps after the relay OCR port. It
keeps public host placeholders, English text, the pending ADS-B time-coverage
change and the existing runtime authority boundaries. It performs no production
deployment, notification, commit or push. File selection and checks are in the
[port record](../../publication/observability-port-20261023.json).

## Selected Changes

- [Thumbnail timing](twitch-thumbnail-monitor.md#provider-and-cache-timing):
  bounded HTTP `Age`, persisted first sight independent of OCR, fresh verified
  timing metrics and a reused HTTP-only localhost client. Missing `Age` stays
  unknown; cache revalidation prevents interpreting the origin-side estimate
  as Twitch rendering delay itself.
- [Input quality](sli-and-dashboard.md#current-input-quality-and-historical-gates):
  latest usable raw OAuth and Prometheus states get a separate
  `source_disagreement_current` diagnostic. Rolling percentages and formal
  gates retain their historical evidence. This does not claim to repair every
  raw/Prometheus disagreement or rewrite lost observations.
- YouTube watchdog: an established FFmpeg connection in a ready direct-backup
  workload counts as local sending while the main sender is unavailable.
  Process, state and exact peer port must match; unrelated HTTPS connections
  do not qualify. Public configuration and destructive-action guards remain.
- [Notification delivery](notification-and-auto-recovery.md#delivery-and-current-state-rules):
  actual-route receipts, ordered recovery, durable overflow, bounded message
  batches, current measurement incidents and distinct planned-rollout events.
- [Monitoring v4 history](../../monitoring-v4/docs/normal-monitoring-history.md):
  schema-v7 normal aggregates, explicit migration campaigns, verified immutable
  history copies, bounded PostgreSQL cursors and matching reporter/restore
  examples. Retention and elapsed-time qualification gates are preserved.

## Boundaries

Confidence and public coverage thresholds, relay OCR rules, ADS-B coverage
semantics, provider requests, live services and CRA are unchanged by this port.
The thumbnail adapter reuses the existing API observation; no extra Helix call
is added. Notification tests use mocked webhooks. Database tests use disposable
state and do not connect to an operated database.

Private host installers, obsolete OCI-route prototypes, operational records,
raw images, logs, databases, credentials and public-player acquisition work are
excluded. Public-only safeguards and documentation are retained. The separate
CRA reflex source was already identical and is not copied again. This is a
selected source reconciliation, not a claim that all private/public differences
should disappear or that this checkout has been deployed.
