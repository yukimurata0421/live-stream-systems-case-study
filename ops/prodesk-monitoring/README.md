# Prodesk Monitoring Extract

This directory contains the small service-level checks that predated the
stream_v3 monitoring split. They are kept as context for the evolution from
single-host service monitoring toward the v3 delivery-plane / observability-plane
model.

Included checks:

- `check_adsb_alive.py`: systemd service liveness check with Discord cooldown.
- `check_graphs1090_alive.py`: service plus generated graph freshness check.
- `check_amazon_mail_notifier.py`: simple external notifier service heartbeat.
- `check_env_center_alive.sh`: sanitized public version of the legacy shell check.

Runtime secrets such as Discord webhooks must be supplied through environment
variables. The original prodesk script contained an inline webhook; it is
intentionally not copied here.
