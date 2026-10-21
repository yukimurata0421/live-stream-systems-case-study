# OCI relay restore bundle

This tool captures the configured OCI relay read-only, encrypts its runtime
files, and verifies or stages them offline. `install-files` copies verified
files into a selected root; it never starts services, changes networking, or
restores recovery authority.

See the [restore runbook](../../docs/v3/oci-backup-and-restore.md) for the
collection prerequisites, file roles, commands, and limits.

The archive contains a static Linux amd64 MediaMTX binary, matching pristine
upstream source, units, required configuration and credentials, and selected
workspace source. Runtime credentials go straight from the SSH stream into
GPG. The public repository contains the tool and synthetic tests, not archives,
keys, images, or production settings.

`rootfs/` contains allowlisted runtime and network files. `reference/` contains
historical state, identity, firewall and read-only observation setup for review;
it is never automatically installed. SSH private keys, administrator key lists,
cloud credentials, control databases, and inactive legacy services are excluded.

Python 3 and GnuPG are needed locally. Collection also requires an existing,
explicitly authorized SSH path and the root-owned source identity configuration
shown in `ops/config/oci_backup_source.example.json`. The source must already
have the supported unit, binary, credential, and observer-release layout.
Copying this example does not configure or authorize a new source.
