# Public Source Language

Public documentation, code comments, docstrings, notifications, and generated
reports use English. Private development documentation may use Japanese.

Run the repository check with:

```sh
python3 ops/scripts/validate_public_language.py
```

The check scans Git-tracked and nonignored publication files. Japanese text fails
unless its exact file and line-content hash is listed in
[`publication/language-policy.json`](../publication/language-policy.json).
Python comments and docstrings cannot receive an exception. CI runs this check
before public boundary validation.

Authored Unicode music metadata, mojibake fixtures, UTF-8 canonicalization
payloads, historical Japanese parser inputs, and language-detection ranges use
explicit Unicode escapes in source. Their runtime strings and matching behavior
are preserved. Descriptions and comments remain English.

The only literal-character exception in the integrated Stream repository is the
unchanged upstream MapLibre Unicode character table. Standalone Monitoring and
recovery-control repositories have no literal Japanese exceptions. New or changed
literal Japanese data requires a reviewed exception; test directories are not
exempt.

The October 14, 2026 localization preserves control flow, identifiers, numeric
thresholds, JSON keys, and historical validation outcomes. Human-readable
notification labels and report text change to English. Localization changes
source hashes; prior source manifests remain available under
`publication/history/`. It does not establish deployment, live recovery, or
formal qualification.
