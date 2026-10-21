# OCI Backup And Files-Only Restore

A replacement relay needs the executable, matching source, units, configuration,
and a way to verify them before starting anything. The
[bundle tool](../../ops/oci-backup/README.md) captures those inputs read-only and
keeps encrypted runtime files separate from public workspace source. Restoring
files is a different result from restoring a live service.

## Contents And Identity

| Part | Contents |
| --- | --- |
| `runtime.tar.gz.gpg` | Runtime/network files, required secrets and a private manifest. |
| `mediamtx-source.tar.gz` | Pristine matching upstream tag and license. |
| `workspace-source.tar.gz` | Explicitly selected monitor/backup source; no cache or runtime evidence. |
| `index.json` | Part sizes and SHA-256, key identity, source version and capture time. |
| `bundle.py`, `collect.py`, `README.md` | Verification, collection and restore procedure. |

Collection checks loaded units, running binary/processes, credentials, persistent
and live configuration, and observer release before and after capture. Runtime
stdout goes directly through SSH into GPG; no plaintext runtime tar is stored.
The current collector expects the supported static Linux amd64 binary and unit
layout. It does not discover or reconfigure an arbitrary cloud VM.

The collection source must have a root-owned, non-writable identity file at
`/etc/stream-v3-oci-backup/source.json`, based on the
[source example](../../ops/config/oci_backup_source.example.json). Its generic
`relay-host` identifier and machine-id file hash must match the source. Existing
SSH host-key verification and authorized root reads are prerequisites; the tool
does not install credentials or sudo rules.

## Verify Without OCI

Python 3 and GnuPG are required. Use an existing private backup root, with
0700 directories and 0600 files. The passphrase stays in a separate private key
directory. No archive or key from the operated system is published here.

```bash
OCI_BACKUP_ROOT="$HOME/.local/share/stream-v3-oci-backups"
python3 "$OCI_BACKUP_ROOT/latest/bundle.py" verify \
  --snapshot "$OCI_BACKUP_ROOT/latest" \
  --key-file "$OCI_BACKUP_ROOT/keys/archive.passphrase"
```

Verification checks part hashes, authenticated decryption, member inventory and
hashes, syntax, unit/credential references, symlink scope and matching binary
version. A failed or incomplete capture is not promoted to `latest`.

## Stage And Review File Placement

Choose new private directories, preferably in tmpfs for a review:

```bash
python3 "$OCI_BACKUP_ROOT/latest/bundle.py" stage \
  --snapshot "$OCI_BACKUP_ROOT/latest" \
  --key-file "$OCI_BACKUP_ROOT/keys/archive.passphrase" \
  --directory /dev/shm/oci-restore-review
python3 "$OCI_BACKUP_ROOT/latest/bundle.py" install-files \
  --directory /dev/shm/oci-restore-review \
  --root /dev/shm/oci-restored-root
```

Only allowlisted `rootfs/` runtime/network files are installed. Different
existing files, symlink parents, altered staged content, traversal, unsupported
links and duplicate members are rejected. `reference/` holds historical keeper
state, source identity, firewall and observation setup; it is never automatically
installed or treated as current authority. Remove only the review-owned plaintext
tree after verification.

## Prepare A New Capture

After checking the actual source and using an existing authorized SSH path:

```bash
python3 ops/oci-backup/bundle.py create \
  --backup-root "$OCI_BACKUP_ROOT" --repository "$PWD" \
  --upstream-repository /path/to/pristine/mediamtx \
  --upstream-tag v1.21.2 --via delivery-host --source relay-host
```

The public SSH names and identity examples are placeholders. Keep failed partial
captures for diagnosis, retain prior snapshots, and promote a new version only
after verification. Source tags are checked; a matching source tag does not prove
bit-for-bit reproduction of the saved executable.

## Actual Recovery Is A Separate Operation

A new VM needs a supported Ubuntu environment, Python, systemd, WireGuard,
iptables and CA certificates. VM/network provisioning, package caches, cloud
credentials, SSH private keys, control databases and prior approvals are outside
the bundle. Review the new host identity, old relay/senders, current configuration,
network endpoints, key trust and rollback before placing files at `/` or starting
services. A prior machine's identity or recorded budget cannot authorize a new one.

`install-files` never starts WireGuard, MediaMTX or the keeper and never enables
units. Starting them requires a separately authorized recovery. Verify source
connection, relay input, per-destination output, public reception and monitor
return independently. The [dated source-port record](public-source-port-2026-10-22.md)
distinguishes offline checks from an actual recovery; remaining operational gaps
belong in [current status](../current-status.md).
