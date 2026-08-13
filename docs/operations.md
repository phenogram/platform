# Phenogram Platform operations guide

This guide describes the runtime that exists in this repository. It assumes a single application instance for the initial production deployment, PostgreSQL with durable storage, and TLS termination in front of the Rust service.

## Runtime inventory

| Component | Responsibility | Persistent state |
| --- | --- | --- |
| `app` | API proxy, management API/UI, Telegram ingress, SSE, retention task, four webhook workers | Owns none outside PostgreSQL; optionally reads the local Bot API volume |
| `postgres` | Accounts, sessions, memberships, encrypted bot credentials, update journal, deliveries, activity and audit data | `postgres-data` volume |
| Caddy or equivalent | TLS, security headers, streaming reverse proxy, access-log redaction | Certificates/logs, depending on deployment |
| Optional `telegram-bot-api` | Official local Telegram Bot API server for eligible plans | `telegram-bot-api-data` volume |

The base Compose file contains PostgreSQL, the application, and Caddy. The application port is published only on loopback; Caddy publishes HTTP/HTTPS and reaches `app:8080` on the Compose network. `deploy/Caddyfile` reads `PHENOGRAM_WEB_DOMAIN` (default `localhost`) and `PHENOGRAM_API_DOMAIN` (default `api.localhost`) and enforces the same host split as production. Direct local access to port 8080 remains a supported one-origin development mode when `WEB_BASE_URL` and `API_BASE_URL` are identical. `compose.premium.yaml` is an optional overlay for the separate official Telegram service.

Application startup is fail-fast: configuration is validated, PostgreSQL is connected, migrations are applied, background tasks are started, and only then is the HTTP listener bound. `SIGTERM` and `SIGINT` stop the HTTP server gracefully. A delivery interrupted after the lease is taken is marked retryable by the retention task once its five-minute lease expires.

## Configuration

| Variable | Required/default | Operational meaning |
| --- | --- | --- |
| `APP_ENV` | `development` | Set exactly to `production` for HTTPS/config checks and secure cookies. |
| `LISTEN_ADDR` | `127.0.0.1:8080` | Compose overrides this with `0.0.0.0:8080`. |
| `WEB_BASE_URL` | Required | Canonical browser origin without a trailing slash. Serves the landing page, console, management `/api/*`, and health endpoint. Must be HTTPS in production; browser `Origin` must match it exactly. Production value: `https://phenogram.io`. |
| `API_BASE_URL` | Defaults to `WEB_BASE_URL` | Canonical machine origin without a trailing slash. Used for Telegram ingress, Telegram-compatible bot/file paths, SSE, and signed file URLs. Production requires a distinct HTTPS host. Production value: `https://api.phenogram.io`. |
| `DATABASE_URL` | Required | PostgreSQL connection URL. The pool uses 2–20 connections per app instance. |
| `MASTER_KEY` | Required, at least 32 bytes | Encrypts bot tokens and webhook secrets after domain-separated derivation. |
| `PUBLIC_ID_KEY` | Required, at least 32 bytes | Derives stable bot public IDs used for token lookup and public routes. |
| `LINK_SIGNING_KEY` | Required, at least 32 bytes | Signs public files and derives CSRF tokens. |
| `TELEGRAM_CLOUD_API_URL` | `https://api.telegram.org` | Cloud Bot API origin; useful to replace only in isolated tests. |
| `TELEGRAM_LOCAL_API_URL` | Unset | Separate local Bot API origin. Local routing is unavailable when unset. |
| `TELEGRAM_LOCAL_DATA_DIR` | Unset | Absolute read-only root containing local Bot API files. Required to serve opaque absolute `file_path` references. |
| `SESSION_TTL_HOURS` | `720` | New session lifetime. |
| `RETENTION_SWEEP_SECONDS` | `3600` | Interval between sweeps. Do not set to zero. |
| `RUST_LOG` | application info | Structured stdout log filter. |
| `POSTGRES_PASSWORD` | development default in Compose | Builds `DATABASE_URL` in the base stack. Never use the default in production. |
| `PHENOGRAM_WEB_DOMAIN` | `localhost` in Compose | Caddy browser/management site address. Production value: `phenogram.io`. |
| `PHENOGRAM_API_DOMAIN` | `api.localhost` in Compose | Caddy machine API site address. Production value: `api.phenogram.io`. |
| `PHENOGRAM_HTTP_PORT`, `PHENOGRAM_HTTPS_PORT` | `80`, `443` | Published Caddy ports. |
| `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | Premium overlay only | Operator-owned Telegram application credentials for the official local server. |

Generate the three application keys independently. Store them and database credentials in the deployment secret manager, not in the image, repository, Compose manifest, shell history, or centralized logs. Production validation rejects short secrets and values containing `development`, but it cannot assess entropy.

## Initial production deployment

1. Provision a supported Docker host or equivalent orchestrator, durable PostgreSQL storage, encrypted backups, DNS, and inbound HTTPS.
2. Copy `.env.example` to an untracked deployment secret file. Set `APP_ENV=production`, `WEB_BASE_URL=https://phenogram.io`, `API_BASE_URL=https://api.phenogram.io`, a strong database password, and three independent application keys.
3. Set `PHENOGRAM_WEB_DOMAIN=phenogram.io` and `PHENOGRAM_API_DOMAIN=api.phenogram.io`, then expose the included Caddy service, or place an equivalent reverse proxy in front of `app:8080`. Preserve SSE streaming. The web host must reject machine routes; the API host must accept only `/bot*`, `/file[/...]`, `/telegram[/...]`, `/events[/...]`, and `/public[/...]`. The supplied Caddyfile enforces that split, enables compression and security headers, replaces the complete URI in access logs, and deletes cookie, authorization, Telegram secret, and CSRF headers from access logs.
4. Restrict direct access to port 8080 and PostgreSQL. Only the reverse proxy should reach the app; only the app and authorized operators should reach the database. The ingress must overwrite, not trust, client-supplied `X-Forwarded-For` and `X-Real-IP`; the bundled Caddyfile does this.
5. Back up PostgreSQL before every upgrade that contains migrations.
6. Build and start the base stack:

   ```sh
   docker compose up --build -d
   docker compose ps
   curl -fsS https://phenogram.io/api/health
   ```

7. Confirm the response is HTTP 200 with `status: "ok"` and `database: true`. Confirm `https://api.phenogram.io/` and `https://api.phenogram.io/api/health` both return 404, and that a machine route on `https://phenogram.io` returns 404. Then register a test account, connect a test bot, make a proxied `getMe` call through `api.phenogram.io`, deliver a real Telegram update, and confirm it appears in the update journal.

The health endpoint checks PostgreSQL only. It does not prove reachability of Telegram, the downstream webhook, SSE fan-out, the optional local server, or disk headroom; keep those as separate checks.

Migrations run automatically on application startup. There is no automated schema downgrade. Review migrations and take a restorable backup before deploying a new binary.

## TLS and log handling

The token-bearing compatibility route intentionally matches Telegram:

```text
/bot{token}/{method}
/file/bot{token}/{file_path}
```

Public SSE URLs contain a stream secret, and signed file URLs contain a reusable signature until expiry. Redact the complete request target—including query parameters—at every hop. The included Caddyfile replaces `request.uri` with `/redacted` and drops `Cookie`, `Authorization`, `X-Telegram-Bot-Api-Secret-Token`, and `X-Phenogram-CSRF`. If a CDN, ingress controller, APM agent, packet capture, or upstream application-error logger is added, verify its behavior with synthetic credentials before production use.

Keep both public DNS names as direct, DNS-only records when using Cloudflare in front of this deployment. A proxy/CDN adds another request-log and buffering layer in front of token-bearing Bot API paths, signed URLs, and SSE. If an edge proxy is introduced later, first prove full-URI redaction, disabled buffering for SSE, large upload/download behavior, and forwarding-header overwrites with synthetic credentials.

Do not log request bodies: updates and Bot API requests may contain personal data. Limit access to application logs because network errors can include destination context even when proxy access logs are scrubbed.

Registration and login allow 30 attempts per source and eight per source/email in a rolling ten-minute window, and at most four Argon2 jobs at once. These controls are in-memory per process. Their source comes from forwarding headers, so a trusted ingress must overwrite those headers; add distributed edge limits before running multiple replicas.

When a connected bot already has a Telegram webhook, its URL is used only in that request to show the owner what would be replaced. Current schema does not retain the URL: migration `0006_remove_plaintext_webhook_url.sql` drops the legacy column, and the expiring audit entry records only whether takeover occurred. Backups created before that migration may still contain the old value and should be handled accordingly.

## Monitoring and readiness

Minimum alerts:

- `https://phenogram.io/api/health` is non-200 or reports `database: false`;
- application or PostgreSQL container is unhealthy/restarting;
- sustained HTTP 5xx/502 responses;
- elevated registration/login 429s or CPU saturation—the built-in limiter and four-slot Argon2 gate are per process;
- PostgreSQL disk, volume inode, connection, or transaction pressure;
- failed retention sweeps;
- growing or old `pending`/`failed` webhook deliveries;
- bots with stale `last_update_at`, `degraded`, or `token_invalid` status;
- expired-row backlog or unusual growth in `updates`, `outbound_messages`, `api_calls`, `conversations`, or `audit_log`;
- SSE 429s: each process allows 256 total streams and four per stream key;
- optional local Bot API process/volume failure.

Useful read-only checks:

```sh
docker compose ps
docker compose logs --tail=200 app
docker compose exec -T postgres psql -U phenogram -d phenogram -c \
  "SELECT state, count(*), min(next_attempt_at) AS oldest FROM webhook_deliveries GROUP BY state ORDER BY state"
docker compose exec -T postgres psql -U phenogram -d phenogram -c \
  "SELECT status, routing_mode, update_mode, count(*) FROM bots GROUP BY 1,2,3 ORDER BY 1,2,3"
docker compose exec -T postgres psql -U phenogram -d phenogram -c \
  "SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size"
```

`api_calls` provides application-level method/status/latency history but no metrics endpoint is included. Export stdout logs and database/storage metrics through the hosting platform. Treat raw update contents as sensitive and avoid shipping them to general-purpose telemetry.

## Backups and restore drills

A usable backup set contains:

1. a consistent PostgreSQL backup;
2. the exact `MASTER_KEY`, `PUBLIC_ID_KEY`, and `LINK_SIGNING_KEY` versions active at backup time;
3. deployment configuration and image/source revision metadata;
4. if used, a consistent snapshot of the local Bot API volume and its Telegram API credentials.

Without the original `MASTER_KEY`, encrypted bot and webhook credentials in PostgreSQL cannot be recovered. Without the original `PUBLIC_ID_KEY`, stored public IDs no longer match token-derived lookups. Restoring with a different `LINK_SIGNING_KEY` invalidates old public file links and changes CSRF values.

Example logical database backup:

```sh
umask 077
mkdir -p backups
phenogram_backup="backups/phenogram-$(date +%Y%m%dT%H%M%S).dump"
docker compose exec -T postgres pg_dump -U phenogram -d phenogram -Fc > "$phenogram_backup"
pg_restore --list "$phenogram_backup" >/dev/null
```

Use encrypted, access-controlled off-host storage and enforce retention independently of the application. A volume snapshot is useful in addition to, not instead of, a logical dump.

Test restores into an isolated PostgreSQL instance with no production traffic. Verify migrations, row counts, a login, decryption through a non-production bot, update history, and signed-link behavior using the backed-up key versions. Never use `--clean` against the live database as a casual restore test.

For the optional local Telegram service, stop or quiesce it while taking a filesystem snapshot of `telegram-bot-api-data`, or use a storage system that provides consistent volume snapshots. Regularly test both service recovery and Telegram re-login; PostgreSQL alone does not contain its local files or runtime state.

The production Helm profile configures six-hourly encrypted off-site Restic
backups with daily/weekly/monthly retention and repository sampling. It does not
provide point-in-time recovery, replication, or cross-region failover. Define
and test RPO/RTO, alert on missed jobs, and repeat isolated restore drills.

## Secret rotation

### Stream keys

Create a replacement in the bot's **Delivery & API** view, move the consumer, verify it, then revoke the old key in the same view. Revocation is immediate for new connections; an open stream rechecks the key about every 15 seconds, emits `revoked`, and closes.

### Downstream webhook secret

Call virtual `setWebhook` through Phenogram with the new `secret_token`, update the receiver atomically, and watch delivery failures. The value is encrypted at rest and sent in `X-Telegram-Bot-Api-Secret-Token`.

### Signed-file/CSRF key

Rotating `LINK_SIGNING_KEY` immediately invalidates every outstanding signed file URL and changes the expected CSRF token for existing sessions. Schedule a maintenance window, rotate the secret, restart all app instances together, and have browser clients reload or fetch `/api/me` before their next mutation.

### Master encryption key

Do not simply change `MASTER_KEY`: the application has no online re-encryption command, and existing bot tokens, ingress/downstream secrets, and outstanding opaque local-file references would become unreadable. A safe rotation requires a purpose-built decrypt/re-encrypt migration while the old key is available, or controlled deletion and reconnection of every bot. Back up first and verify a restore.

### Public-ID key

Do not rotate `PUBLIC_ID_KEY` in place. Proxy authentication derives a public ID from the presented bot token and looks up the stored ID; a new key breaks every bot route and leaves Telegram pointing at old ingress URLs. Rotation requires a coordinated database/public-URL migration and upstream webhook reprovisioning, which this MVP does not automate.

### Bot tokens

Revoke a suspected token with @BotFather immediately. This MVP has no in-place token replacement endpoint. Deleting and reconnecting the bot removes its Phenogram history through cascading deletes, so preserve required evidence under the applicable policy before that destructive step.

### Database and local-server credentials

Rotate the PostgreSQL role secret together with `DATABASE_URL` and restart the app. For `TELEGRAM_API_ID`/`TELEGRAM_API_HASH`, follow Telegram's credential process, update the companion service secret, and test bot login and update ingress before routing traffic.

## Plan administration and retention

Registration assigns the `free` plan. There is no payment provider or admin endpoint in this MVP; the plan-selection UI is informational. Until billing exists, authorized operators assign plans directly in PostgreSQL and should record the approval externally and in the audit log.

Inspect before changing a membership:

```sql
SELECT users.id, users.email, memberships.plan_id, memberships.status,
       plans.bot_limit, plans.retention_days, plans.local_bot_api
FROM users
JOIN memberships ON memberships.user_id = users.id
JOIN plan_definitions plans ON plans.id = memberships.plan_id
WHERE users.email = 'owner@example.com';
```

Apply a reviewed assignment in a transaction, using `free`, `pro`, or `scale`:

```sql
BEGIN;
UPDATE memberships
SET plan_id = 'pro', status = 'active', updated_at = now()
WHERE user_id = '<user-uuid>';
INSERT INTO audit_log (user_id, action, metadata)
VALUES ('<user-uuid>', 'membership.admin_changed', '{"plan_id":"pro"}'::jsonb);
COMMIT;
```

A downgrade does not disable excess existing bots; it only blocks additional bot inserts. Resolve the intended active set before applying a lower limit.

Plan retention is calculated when `updates`, `outbound_messages`, `api_calls`, bot-scoped `audit_log` rows, and conversation projections are created or refreshed. Plan changes are not retroactive for already stamped rows. Account-scoped audit rows use a one-year expiry, sessions use `SESSION_TTL_HOURS`, and delivery rows cascade with their parent updates. Every retention run drains each expired table completely in repeated 5,000-row batches, yielding between full batches.

Monitor expired backlog:

```sql
SELECT count(*) AS expired_updates FROM updates WHERE expires_at <= now();
SELECT count(*) AS expired_outbound FROM outbound_messages WHERE expires_at <= now();
SELECT count(*) AS expired_api_calls FROM api_calls WHERE expires_at <= now();
SELECT count(*) AS expired_conversations FROM conversations WHERE expires_at <= now();
SELECT count(*) AS expired_audit FROM audit_log WHERE expires_at <= now();
```

## Optional local Telegram Bot API service

The companion image builds a pinned revision of Telegram's official `tdlib/telegram-bot-api` and starts it with `--local` on port 8081. The platform itself remains the gateway; it does not implement Telegram's server. The premium overlay gives the companion read/write access to `telegram-bot-api-data`, mounts the same volume read-only into the app, and configures both the API origin and local file root.

Obtain `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` using Telegram's [official instructions](https://core.telegram.org/api/obtaining_api_id), store them as secrets, then start the overlay:

```sh
docker compose -f compose.yaml -f compose.premium.yaml up --build -d
docker compose -f compose.yaml -f compose.premium.yaml ps
docker compose -f compose.yaml -f compose.premium.yaml logs --tail=200 telegram-bot-api
```

Before migrating a bot:

- confirm its membership has `local_bot_api = true`;
- confirm the companion service and persistent volume are healthy and backed up;
- review Telegram's [local Bot API server behavior](https://core.telegram.org/bots/api#using-a-local-bot-api-server);
- schedule the migration, because moving to local calls cloud `logOut`, and returning to cloud can be subject to Telegram's login cooldown;
- use the confirmed migration control in bot settings (or call the routing API with `mode: "local"` and `confirm_migration: true`);
- call `getMe` through Phenogram and verify a real incoming update end to end;
- query the actual local server's `getWebhookInfo` securely and confirm it points to Phenogram ingress.

Routing changes are serialized per bot with a PostgreSQL advisory lock. Phenogram logs out of the old backend, records the new mode, and installs its managed ingress webhook on the target backend. Success sets the bot healthy; a target that is not ready leaves the selected mode in place with degraded status. Retry webhook installation through `POST /api/bots/{bot_id}/provision`, then repeat `getMe`, upstream `getWebhookInfo`, and real-update checks.

In local mode, Phenogram virtualizes `getFile`: an absolute path returned by the companion is replaced with an authenticated-encrypted `__phenogram_local__/...` reference scoped to that bot. Token-bearing and signed public download routes decrypt it, canonicalize the path, require it to remain below `TELEGRAM_LOCAL_DATA_DIR`, and stream from the read-only mount. Single byte ranges return `206`; invalid or multiple ranges return `416`. Never make the app's mount writable.

The companion service is still an external operational dependency. The Rust application does not monitor its internals, schedule its backups, rotate Telegram application credentials, or provide a general local-server control plane.

## Incident runbooks

### Health is degraded or PostgreSQL is unavailable

1. Stop rollouts and confirm whether `https://phenogram.io/api/health` is 503.
2. Inspect container health, PostgreSQL logs, storage capacity/inodes, connection count, and host pressure.
3. Preserve logs and take a snapshot before repairing suspected corruption.
4. Restore connectivity or fail over PostgreSQL, then verify migrations and `https://phenogram.io/api/health`.
5. Check delivery leases. Jobs left `delivering` are automatically returned to `failed` after five minutes by the retention task.
6. Run a proxied test call and a real update ingress check; database health alone is insufficient.

### Telegram updates stop arriving

1. Check bot `status` and `last_update_at` in the console/database.
2. Distinguish the two webhooks: Phenogram's virtual `getWebhookInfo` reports the developer's downstream destination, not Telegram's upstream destination.
3. Using secure operator tooling, call `getWebhookInfo` directly on the selected Telegram cloud/local backend and verify the URL is `${API_BASE_URL}/telegram/webhook/{public_id}`.
4. Check reverse-proxy status codes for the ingress route, TLS/DNS reachability, and 401 responses caused by a secret mismatch.
5. Send one controlled test update and verify it appears in `updates` before inspecting downstream delivery.
6. Retry the managed upstream webhook with `POST /api/bots/{bot_id}/provision`; it decrypts the existing ingress secret and installs the webhook on the currently selected backend without deleting history. Confirm the bot returns to `healthy`.

### A downstream webhook is failing

1. Inspect virtual `getWebhookInfo`, `webhook_deliveries`, and the last error/status.
2. Verify public DNS, TLS chain, allowed port, response latency below the 15-second client timeout, and that every resolved address is globally routable. For every attempt, Phenogram disables inherited proxies, resolves once, validates all addresses, and pins the request to that set; infrastructure egress policy should remain in place.
3. Verify the receiver accepts JSON and the current secret header.
4. Correct the receiver or call virtual `setWebhook` with the replacement URL/secret. Existing failed jobs use the current stored destination on their next attempt.
5. Watch retries. Backoff grows exponentially to one hour; there is no manual retry endpoint or terminal retry count. The job is eventually removed when its parent update reaches retention expiry.

### SSE misses or duplicates events

1. Treat 401 as a missing, revoked, or mismatched stream URL and issue a replacement if needed.
2. Persist each SSE event ID only after the consumer commits the event.
3. Reconnect with `Last-Event-ID`; replay is ordered by database row ID and capped at 5,000 rows, 8 MiB serialized total, and roughly 2 MiB for one event.
4. On a `resync` event, persist its event ID and reconnect from that ID to continue the next replay page. Reconcile through the authenticated update-history API if necessary.
5. Confirm the reverse proxy is not buffering and permits connections longer than the 15-second keepalive interval.
6. Treat 429 as a connection-cap signal. Each process allows 256 total streams and four per stream key; permits are held for the connection lifetime.
7. The live bus is process-local. A stream on replica A does not see an update ingested by replica B until reconnect/replay. Keep the MVP to one app instance, or add a shared notification layer such as PostgreSQL `LISTEN/NOTIFY`, Redis, or NATS before scaling live SSE horizontally.

### Signed file download fails

1. Check that expiry is current and no more than seven days in the future.
2. Use the exact `file_path` returned through Phenogram; path, public ID, and expiry are covered by the signature. Local absolute paths are returned as opaque `__phenogram_local__/...` references.
3. Confirm the bot still exists, its token decrypts, and its selected Telegram backend is reachable.
4. For an opaque local reference, confirm `TELEGRAM_LOCAL_DATA_DIR` is an absolute path, the companion volume is mounted there read-only, and the decrypted file still canonicalizes below that root.
5. Do not extend a link by editing `expires`; create a new signed URL.

### Local routing migration fails

1. Do not repeatedly toggle routing. Determine whether cloud `logOut` already succeeded.
2. Check the local service logs, Telegram API credentials, volume ownership/capacity, and `getMe` directly on the local service.
3. If the platform returned a provisioning warning, keep traffic stopped and retry `POST /api/bots/{bot_id}/provision` until the bot is healthy; then confirm `getMe` and the Phenogram ingress webhook on the target backend.
4. When returning to cloud, expect Telegram's cooldown and validate both proxied methods and upstream update delivery after it expires.
5. Preserve the local volume until recovery is complete.

## Current production boundaries

- `phenogram.io` is the browser, landing, console, management API, and health surface. `api.phenogram.io` is restricted to Telegram-compatible bot/file paths, Telegram ingress, SSE, and signed public downloads; it intentionally does not expose `/`, `/api/health`, or management endpoints.
- SSE is the only new subscription transport. Kafka, WebSockets, NATS, and similar brokers are not implemented.
- Live SSE fan-out and its 256-global/four-per-key connection caps are process-local. Replay is durable but capped per connection at 5,000 rows, 8 MiB total, and roughly 2 MiB per event.
- Downstream delivery uses four workers per app process. Stored `max_connections` is informational in this MVP.
- Payment collection, provider reconciliation, tax/invoice handling, self-service plan changes, and an admin console are absent.
- Plan retention covers updates, outbound messages, API calls, conversation projections, and bot-scoped audit rows, but changing a plan does not recalculate already stamped expiry times. Account-scoped audit rows use one year.
- The optional official local Bot API server is a separate operational component. Routing, managed webhook reprovisioning, opaque local paths, and range delivery are integrated, but the platform is not a local-server monitoring or backup control plane.
- Bot credential replacement is absent. Reconnecting after a BotFather token rotation requires deleting the existing bot record, which cascades its stored history.
- Authentication has per-process login/registration throttling and a four-job Argon2 gate, but no MFA, email verification, password reset, organization model, or RBAC. The source limiter trusts ingress-supplied forwarding headers and is not distributed across replicas.
- Downstream destination pinning and global-address checks reduce SSRF risk, but operator-level egress controls remain required defense in depth.
- The Helm deployment includes scheduled encrypted PostgreSQL backups to an operator-provisioned S3-compatible bucket, but no built-in metrics endpoint, point-in-time recovery, multi-region failover, or automated disaster recovery.
- Compatibility has been exercised for the included contract tests, not certified against every Telegram method/content type. Managed update methods intentionally implement a subset of Telegram parameters.
- Managed `setWebhook` does not accept multipart custom certificates or implement `ip_address`; `max_connections` is stored/reported but does not control the global four-worker pool.

These boundaries make the safest initial shape a protected, single-instance deployment with managed PostgreSQL/backups, strict ingress redaction and rate controls, a small authorized user base, and explicit operator ownership of plans and premium routing.
