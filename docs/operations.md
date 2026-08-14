# Phenogram Platform operations guide

This guide describes the production architecture deployed to the `phenogram` namespace on Contabo. The central invariant is simple: the pinned official Telegram Bot API server owns polling and webhook delivery, while Phenogram's journal is a nonblocking observer. PostgreSQL, the collector, and the console must be able to fail without becoming prerequisites for native update delivery.

## Runtime inventory

| Component | Responsibility | Persistent state |
| --- | --- | --- |
| `phenogram` application | Landing page, authenticated console, management API, OAuth, lifecycle jobs, retention, SSE replay, Bot View calls | PostgreSQL only |
| PostgreSQL | Provider identities, sessions, memberships, encrypted control-plane bot credentials, observer journal, projections, API activity, audit data | 10 GiB PVC in production |
| Data-plane gateway | Authenticates token-derived routes from an in-memory last-good snapshot and streams `/bot*` requests/responses | 256 MiB snapshot PVC; no raw-token list |
| Official `standard` pool | Pinned `tdlib/telegram-bot-api` without `--local`; owns normal Bot API sessions, `TQueue`, polling, webhooks, retries, and acknowledgements | 20 GiB native Bot API PVC |
| Official `local` pool | Same pinned binary with Telegram `--local`; owns local-mode sessions and files | 100 GiB native Bot API PVC |
| File sidecar, one per pool | Authenticated read-only HTTP bridge for `/file`, because the official binary has no public file endpoint | None; reads its pool PVC |
| Tap collector, one per pool | Reassembles best-effort update mirrors, commits ACKed managed-lifecycle receipts, and writes journal/projections | None outside PostgreSQL |
| ingress-nginx | TLS, strict host routing, request streaming, token-path log suppression | Kubernetes TLS Secrets |

Each official pool has one replica and is the only writer to its PVC. Collector and file-sidecar health are deliberately excluded from the official service's readiness: an observer or download-sidecar failure must not remove the official Bot API endpoint.

## Public topology

- `https://phenogram.io` serves the landing page and privacy policy.
- `https://app.phenogram.io` serves the console, same-origin management API, and health endpoint.
- `https://api.phenogram.io` serves `/bot*`, `/file*`, `/telegram/*`, `/events/*`, and `/public/*` only.

The three Cloudflare records stay DNS-only and point to the Contabo ingress. Do not introduce another proxy until full-URI redaction, disabled buffering, long polling, multipart uploads, large downloads, SSE, and forwarding-header handling have been verified with synthetic credentials.

Telegram-compatible paths contain the raw bot token:

```text
/bot{token}/{method}
/bot{token}/test/{method}
/file/bot{token}/{file_path}
/file/bot{token}/test/{file_path}
```

The API ingress disables access logging and sends its nginx error log to `/dev/null`. It also disables request and response buffering, has no aggregate body-size cap, preserves double slashes required by absolute local-mode paths, and permits the larger request head used by official Bot API clients. Keep this behavior when changing ingress-nginx.

## Configuration and secret contract

The control application validates these public and application settings:

| Variable | Production meaning |
| --- | --- |
| `APP_ENV=production` | Enables production origin checks and secure cookies. |
| `LANDING_BASE_URL=https://phenogram.io` | Canonical landing origin. |
| `APP_BASE_URL=https://app.phenogram.io` | Canonical console/management origin and exact browser Origin. |
| `API_BASE_URL=https://api.phenogram.io` | Canonical Bot API, file, SSE, and public-link origin. |
| `DATABASE_URL` | PostgreSQL connection URL. |
| `MASTER_KEY` | At least 32 bytes; encrypts the application-database copy of bot and lifecycle secrets. |
| `PUBLIC_ID_KEY` | At least 32 bytes; derives production/test-domain-separated token lookup IDs. |
| `LINK_SIGNING_KEY` | At least 32 bytes; signs public files and derives CSRF tokens. |
| `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET` | Dedicated sign-in-only Google Web application. |
| `GITHUB_OAUTH_CLIENT_ID`, `GITHUB_OAUTH_CLIENT_SECRET` | Dedicated sign-in-only GitHub OAuth App. |
| `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | Operator Telegram application credentials used by both official pools. |
| `DATA_PLANE_SYNC_TOKEN` | Independent random bearer value of at least 32 bytes for route snapshots, telemetry, and internal file hops. |

Helm expects existing Secrets rather than rendering secret values:

- `phenogram-secrets` contains the variables above plus `POSTGRES_PASSWORD`;
- `phenogram-ghcr` permits image pulls from `ghcr.io/phenogram`;
- `phenogram-backup-secrets` contains the currently enabled PostgreSQL Restic target credentials;
- `phenogram-io-tls`, `app-phenogram-io-tls`, and `api-phenogram-io-tls` are managed by cert-manager.

GitHub Actions receives the namespace-scoped `KUBECONFIG` and repository secret `DATA_PLANE_SYNC_TOKEN`. The workflow writes the data-plane value to a protected temporary file, patches only that key into `phenogram-secrets`, and removes the file. The value is never passed as a Helm parameter or stored in Helm release history.

The chart renders NetworkPolicy resources, but Contabo's current Minikube bridge CNI does not enforce them. Treat `DATA_PLANE_SYNC_TOKEN` as the actual internal authorization boundary until a policy-capable CNI is installed; do not claim that the rendered policies provide live isolation.

## Social OAuth setup

Phenogram authenticates a person by provider plus stable provider subject ID. It stores the display name/handle and avatar URL needed by the console. It does not request, receive, or persist email addresses, access tokens, or refresh tokens.

Google configuration:

1. Create a dedicated Web application OAuth client.
2. Set the homepage to `https://phenogram.io` and privacy policy to `https://phenogram.io/privacy`.
3. Register exactly `https://app.phenogram.io/api/auth/oauth/google/callback`.
4. Request only `openid profile`; do not add `email`.
5. Store the client ID and secret in `phenogram-secrets`.

GitHub configuration:

1. Create a dedicated OAuth App for Phenogram.
2. Set the homepage to `https://phenogram.io`.
3. Register exactly `https://app.phenogram.io/api/auth/oauth/github/callback`.
4. Leave the requested OAuth scope empty; do not request `user` or `user:email` and do not call the email endpoint.
5. Store the client ID and secret in `phenogram-secrets`.

Use separate OAuth clients for local development.

## Direct production deployment

Application deployment is owned by this repository. It does not use Flux, and project-specific releases must not be added to `infra-flux`.

A push to `master` runs:

1. Rust formatting, lint, tests, browser syntax checks, action lint, and strict Helm rendering;
2. immutable Linux/amd64 builds for the application, pinned official server, and streaming gateway;
3. direct `helm upgrade --install` against the Contabo `phenogram` namespace using the namespace-limited kubeconfig;
4. Kubernetes rollout checks and origin-pinned checks for all three public hosts and OAuth redirects.

The production workflow checks the Kubernetes API endpoint and namespace before deployment and rejects a credential that can create namespaces. Image references are digest-pinned. Normal control-plane releases do not implicitly move the independently pinned official, collector, file-sidecar, or gateway images.

## Destructive first data-plane cutover

There is no legacy bot migration in this release. Existing bot records, their captured update history, and the old Telegram sidecar state are disposable. User identities and current browser sessions are separate control-plane records and remain intact.

Before the first release, delete every disposable legacy bot record and its
cascaded bot data. Preserve user identities and browser sessions. Then use three
releases so the new official pools are warm before the public ingress moves:

1. **Build without traffic change.** Keep `dataPlane.enabled=false`, `dataPlane.publicCutover=false`, and `telegramBotApi.enabled=true`. Merge to `master`; the workflow publishes the new app, official-server, and gateway images. Record their immutable digests.
2. **Start the shadow plane.** Pin the recorded gateway, official, collector, and file-sidecar digests in production values. Set `dataPlane.enabled=true`, keep `dataPlane.publicCutover=false`, and leave the legacy sidecar enabled. Verify both official pools, gateway snapshot readiness, collector isolation, files, and a disposable canary route. Do not attempt to copy a legacy Telegram data directory or preserve a legacy Bot API session.
3. **Move the public paths.** Set `dataPlane.publicCutover=true` and `telegramBotApi.enabled=false` with the same pinned data-plane digests. Helm rejects a cutover that leaves the legacy sidecar enabled. Verify `/bot*` and `/file*` through the Contabo origin, then delete the obsolete legacy Telegram PVC. That PVC deletion is irreversible and intentionally discards the old test-bot session/state.

No database backup, bot replay, compatibility shim, per-bot migration, or fallback route is a prerequisite for this empty-bot destructive cutover. The shadow phase exists only to prove that the already-built replacement is healthy before ingress moves; it is not a data-migration phase.

## Release validation

Render the chart before merging:

```sh
helm lint deploy/helm/phenogram-platform \
  --values deploy/helm/phenogram-platform/values-production.yaml

helm template phenogram deploy/helm/phenogram-platform \
  --namespace phenogram \
  --values deploy/helm/phenogram-platform/values-production.yaml \
  --set-string image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set-string telegramBotApi.image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
```

When `dataPlane.enabled=true`, also provide independent gateway, official, collector, and file-sidecar digests. The chart refuses missing pins and refuses `publicCutover=true` while the legacy Telegram sidecar is enabled.

Minimum live checks after cutover:

- `GET https://app.phenogram.io/api/health` is 200, reports PostgreSQL healthy, and exposes the deployed commit revision;
- landing and privacy routes work only on `phenogram.io`;
- management routes work only on `app.phenogram.io`;
- an unknown `/bot.../getMe` route on `api.phenogram.io` returns Telegram-shaped 401 without revealing the marker token in ingress logs;
- an active canary route reaches the official server for JSON, form, multipart, long-poll, large request target, and test-DC requests;
- `/file` supports `GET`, `HEAD`, open-ended, suffix, single, and multipart byte ranges;
- stopping the collector does not change the official server's readiness, restart count, or native API response;
- Google redirects contain only `openid profile`; GitHub redirects contain no scope.

`/api/health` proves the control application and PostgreSQL only. It does not prove gateway snapshot freshness, official-server Telegram connectivity, file-sidecar access, observer completeness, or public ingress behavior.

## Compatibility path

The gateway structurally parses only enough of the token-bearing path to authenticate and choose a pool. It then forwards the original raw path/query, method, end-to-end headers, and streaming body to the pinned official server. It does not interpret method arguments and it does not special-case `getUpdates`, `setWebhook`, `deleteWebhook`, or `getWebhookInfo`.

Never add request-path database access, whole-body parsing, upload buffering, automatic retries, or observer acknowledgement to the gateway. A retry of a partially streamed multipart request is not safe. Token-free telemetry uses a bounded asynchronous queue and may be dropped rather than delay the Bot API response.

The official binary is pinned by commit and patched only with the opt-in observer described in [`deploy/telegram-bot-api/UPDATE_TAP_PROTOCOL.md`](../deploy/telegram-bot-api/UPDATE_TAP_PROTOCOL.md). When upgrading it, re-run raw wire compatibility checks instead of assuming a green control-plane health check is sufficient.

## Connecting a bot and preserving its webhook

Connection starts against Telegram cloud so ownership can be verified before a route is published. The lifecycle operation moves that clean bot session into the selected official pool using Telegram's required `logOut`/`close` behavior.

If `getWebhookInfo` reports an existing webhook, Phenogram does not offer a skip/takeover checkbox. It will transfer the webhook as part of connection, but it fences the operation before mutation until the user either enters the current `secret_token` or explicitly confirms that the webhook has no secret. If Telegram reports a current IPv4 address, the user must also choose whether to preserve it as a fixed `ip_address` or keep DNS resolution; Telegram does not reveal which mode created the reported address. Uploaded certificate bytes are likewise unavailable, so custom-certificate webhooks remain fenced.

If Telegram rejects a token, return a readable invalid/revoked-token message and keep the token in the browser field so the user can correct it. Never include the token in error text, logs, audit metadata, or telemetry.

## Managed-bot discovery and rotation

The native patch appends a compact type-2 lifecycle record to a reserved persistent observer queue after the official `managed_bot` update path has run. This record is independent of `allowed_updates`, so Phenogram can discover a child even when the bot application's native subscription filters that update type. The official server replays the queue head until the collector commits an idempotent receipt and managed job in one PostgreSQL transaction and returns an exact ACK. The worker then calls `getManagedBotToken`, validates the returned token with `getMe`, encrypts the PostgreSQL copy, and connects the child to the manager's current pool.

The job table contains identity and generation metadata, not the child token. A child belonging to another Phenogram account is an ownership conflict and is never moved automatically. Removing a manager leaves children visible as manager-missing; reconnecting the same Telegram manager repairs the relationship.

Managed lifecycle replay is bounded to 10,000 pending records with seven-day expiry. A lost datagram, lost ACK, collector restart, or temporary PostgreSQL outage therefore delays discovery instead of losing it. Queue overflow, expiry, or native TQueue persistence failure still fails open so Telegram delivery remains unaffected; alert on the dedicated lifecycle durability counters. Ordinary type-1 update mirrors remain best-effort and are never ACKed.

The lifecycle observer is an ordered global queue per official pool. Before enabling or cutting over, require `managed_lifecycle_pending` to drain and the overflow, persistence-error, expiry, and ACK-error counters to remain zero. An unresolvable head indicates manual database corruption or an unsupported out-of-band bot removal and can delay later lifecycle records until that head expires.

For a managed token rotation:

- polling mode can rotate through the serialized official-server lifecycle;
- an active native webhook blocks the rotation before route withdrawal, `deleteWebhook`, `logOut`, or `close`;
- the existing route and webhook remain active;
- the bot is surfaced as requiring webhook-secret recovery because Telegram does not reveal the secret/certificate/original pinned IP.

Do not silently recreate an authenticated webhook without its original authentication material.

## Observer journal, SSE, and retention

For each complete type-1 tap event that reaches the collector, Phenogram writes the canonical update and conversation projection in one PostgreSQL transaction. A post-commit trigger emits `NOTIFY`; every application process listens, reloads the committed row, and wakes local SSE subscribers.

This yields live UI updates without manual refresh and database-backed reconnect replay. It does not make the observer lossless. The collector can drop malformed, oversized, incomplete, or unavailable-database copies while native Telegram delivery continues.

SSE consumers must:

1. persist an event ID only after committing the event locally;
2. reconnect with `Last-Event-ID`;
3. reconnect from the ID carried by `resync` after a replay cap or local consumer lag;
4. treat 401 as a missing/revoked key and 429 as a connection-cap response.

Each application process permits 256 public streams globally and four per stream key. Console streams have equivalent bounded local permits. Stream access is revalidated approximately every 15 seconds.

Plan coverage is deterministic. Directly connected bots consume capacity first. Every managed bot on Free, every managed bot beyond paid capacity, and every child without a connected manager receives one-day Phenogram retention and a visible warning. Discovery is never rejected because of plan capacity. Retention deletes observer records only; it does not acknowledge or delete the official server's native updates.

## Standard and `--local` file semantics

The standard pool is the public Bot API compatibility target. Its `/file` request uses the relative `file_path` returned by native `getFile`, and the sidecar resolves it under the raw-token directory on the official PVC.

The local pool keeps Telegram's documented `--local` contract. `getFile` may return an absolute path under `/var/lib/telegram-bot-api`; that path refers to the Contabo pool PVC. It never refers to the bot application's machine or a developer workstation. Ingress preserves the double slash before an absolute path, and the sidecar also accepts only the exact configured root form if an intermediary merges it.

The sidecar performs descriptor-relative, no-follow, same-filesystem opens and streams regular files only. It accepts `GET`/`HEAD` and valid byte ranges. It reads the pool PVC but never writes it. The internal gateway-to-sidecar request is authenticated with `DATA_PLANE_SYNC_TOKEN`.

## Credential storage boundary

Application and official-server storage have different properties:

- PostgreSQL bot tokens, ingress secrets, downstream/lifecycle webhook secrets, and lifecycle snapshots are encrypted by the application with XChaCha20-Poly1305 and per-record associated data.
- Official Bot API PVC storage is exactly the pinned upstream format. It may contain the complete bot token in directory names and persisted runtime state. Production directories include `<raw-token>/`; test-DC directories include `<raw-token>:T/` (with the upstream filesystem fallback where applicable).
- There is no `OFFICIAL_STORAGE_KEY`, official-storage KDF, token-directory HMAC, custom TDLib database key, or Phenogram encrypted envelope around this PVC.

Treat node shell access, PVC mounts, CSI snapshots, storage-system copies, and official-volume backups as bot-token access. Restrict and audit them accordingly. Rotating `MASTER_KEY` has no effect on the official PVC, and losing `MASTER_KEY` does not make the official PVC unreadable.

The currently enabled scheduled Restic job backs up PostgreSQL application state only. It is not part of the destructive first bot cutover and does not claim to protect or encrypt official Bot API PVC contents.

## Monitoring

Minimum alerts and dashboards should cover:

- app health and PostgreSQL availability;
- gateway readiness and last-good route snapshot age/generation;
- official standard/local pod readiness and restart count;
- native upstream status/latency from bounded, token-free gateway telemetry;
- tap sent/dropped/error counters and collector malformed/incomplete/drop logs;
- official PVC capacity/inodes;
- growing or repeatedly retried `managed_bot_sync_jobs`;
- bots in `degraded`, `token_invalid`, provisioning, or manual-recovery state;
- SSE disconnect/429 rates and observer-journal retention backlog;
- ingress 5xx/502 without logging the request target.

Do not use a growing journal count as proof of native delivery, and do not use successful native delivery as proof that every observer copy was stored. They are intentionally separate paths.

## Incident runbooks

### PostgreSQL or the collector is unavailable

1. Verify an already routed bot still receives a native official-server response through `/bot*`.
2. Confirm gateway readiness is using its last-good route snapshot.
3. Confirm the official container remains ready and has not restarted because a sidecar failed.
4. Repair PostgreSQL/collector independently.
5. Expect possible gaps in history, SSE, and conversation projections for the outage interval. Managed lifecycle records replay after recovery while they remain inside the bounded seven-day queue; inspect its pending/overflow/expiry/error counters. Never block native traffic to manufacture observer copies.

New connections, route changes, Bot View, and new route-snapshot publication require the control plane and may be unavailable while PostgreSQL is down. Existing last-good routes and official native delivery do not.

### Native polling or webhook delivery stops

1. Call the affected method through the public gateway and directly against the pool service from an authorized in-cluster shell; compare native responses.
2. Check gateway route generation and pool selection, official process logs, Telegram connectivity, PVC capacity, and pod restart history.
3. Inspect the official server's native `getWebhookInfo`; there is no Phenogram virtual webhook status.
4. Test with a controlled update. The journal is supporting evidence only and may be incomplete.
5. Do not redirect delivery through the collector or PostgreSQL as a workaround.

### Journal or live SSE has a gap

1. Check tap drop/error counters, collector restarts, datagram reassembly expiry, PostgreSQL availability, and retention expiry.
2. Reconnect SSE with the last committed event ID and follow `resync` pagination.
3. Reconcile against the bot application's native source of truth if completeness matters.
4. Accept that a dropped best-effort copy cannot always be reconstructed after Telegram has natively delivered and acknowledged it.

### A managed token rotation is blocked

1. Confirm the child's current native webhook remains configured and its route was not withdrawn.
2. Obtain the webhook authentication material from the bot operator; do not infer it from `getWebhookInfo`.
3. Re-enter/reconfigure the webhook through a controlled lifecycle operation.
4. Retry rotation only after the control plane can preserve the receiver's authentication semantics.

### `/file` fails

1. Use the exact `file_path` returned by the selected pool's native `getFile`.
2. Confirm route pool, file-sidecar process, read-only PVC mount, and `DATA_PLANE_SYNC_TOKEN` agreement.
3. For `--local`, confirm the absolute path is under the configured Contabo data root; a developer-machine path is invalid by design.
4. Test `HEAD` and a small range before attempting the complete download.

### Data-plane control token rotation

Update the repository secret and reconcile the Kubernetes Secret, then restart the gateway, application, collectors, and file sidecars together so every internal participant uses the same value. The official Bot API containers do not consume this secret and must not be restarted merely to rotate it.

## Current production boundaries

- The standard compatibility target is the pinned official server. Phenogram's added hop can still fail at ingress, route authentication, or transport; compatibility must be regression-tested at the raw HTTP boundary.
- The gateway and each official pool are single replica in the MVP, so node/pod disruption can interrupt service even though observer failures are isolated.
- Observer history, Bot View projections, and SSE use best-effort type-1 copies and can contain gaps. Managed discovery uses a bounded ACKed seven-day observer queue; overflow, expiry, or local persistence failure can still create a gap but never blocks native Telegram delivery.
- `--local` absolute paths are Contabo server-local paths, not client-local paths.
- Moving an already active bot between standard and local pools is not exposed as an ordinary routing change.
- A managed token rotation with an active webhook requires explicit recovery of authentication settings.
- Payment collection, self-service upgrades, MFA, organizations/RBAC, multi-region failover, and point-in-time recovery are not implemented.
- Authentication depends on Google and GitHub availability; Phenogram still requests and stores no email addresses or provider tokens.
