# Security And Secrets

The public repository must never contain production credentials or runtime
state.

## Never Commit

- YouTube stream keys
- YouTube API keys
- OAuth client secrets
- OAuth refresh tokens
- Discord webhooks
- SSH private keys
- real Kubernetes Secrets
- `.state/` runtime evidence
- local media files
- production logs and screenshots

## Expected Secret Flow

Use local untracked files, environment variables, or Kubernetes Secrets. Template
files may use placeholder values such as:

```text
REPLACE_WITH_YOUTUBE_STREAM_KEY
REPLACE_WITH_CLIENT_SECRET
REPLACE_WITH_REFRESH_TOKEN
```

## Before Publishing

Run a secret scan, review ignored files, and verify the Git status manually.
The public snapshot already ignores common runtime and credential paths, but the
final responsibility is always with the publisher.
