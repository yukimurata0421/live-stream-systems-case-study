# Runbooks

## Shadow Validation

```bash
python3 ops/scripts/validate_k3s_manifests.py
python3 ops/scripts/v3_shadow_acceptance.py
```

Expected result: manifests pass, control-loop tasks pass, and the shadow action
plan has `execute=false`.

## Runtime Health Check

Check:

- Pod readiness
- FFmpeg ingest connection
- YouTube public state
- YouTube health
- audio route
- now-playing freshness
- memory guardrail
- recovery action plan

## k3s / Node Recovery

For a single-node k3s deployment, distinguish:

- k3s service restart
- Pod restart
- node reboot
- disk loss
- v2 rollback

RTO and RPO should be measured from fault injection to externally visible
recovery, not only from process restart completion.
