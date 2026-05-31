## Scope

This repository contains open-source code published as a public case study, not
a supported OSS product or general-purpose starter. Keep the change focused on
documentation clarity, public validation, sanitized examples, or narrow safety
fixes.

## Checklist

- [ ] I did not include stream keys, OAuth tokens, Discord webhooks, SSH keys,
      private hostnames, internal IPs, raw `.state/`, logs, media, or local
      captures.
- [ ] I preserved the delivery-plane / observability-plane ownership split.
- [ ] I ran `python3 ops/scripts/validate_k3s_manifests.py`.
- [ ] I ran `python3 ops/scripts/v3_shadow_acceptance.py` if runtime,
      monitoring, or recovery behavior changed.
- [ ] I explained the operational reason for the change.
