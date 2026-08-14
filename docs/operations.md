# Phenogram Platform operations guide

This guide describes the runtime that exists in this repository. It assumes a single application instance for the initial production deployment, PostgreSQL with durable storage, and TLS termination in front of the Rust service.

## Runtime inventory

| Component | Responsibility | Persistent state |
| --- | --- | --- |
| `app` | API proxy, management API/UI, Telegram ingress, SSE, retention task, four webhook workers | Owns none outside PostgreSQL; optionally reads the local Bot API volume |
| `postgres` | Provider identities, sessions, memberships, encrypted bot credentials, update journal, deliveries, activity and audit data | `postgres-data` volume |
| Caddy or equivalent | TLS, security headers, streaming reverse proxy, access-log redaction | Certificates/logs, depending on deployment |
| Optional `telegram-bot-api` | Official local Telegram Bot API server for eligible plans | `telegram-bot-api-data` volume |

The base Compose file contains PostgreSQL, the application, and Caddy. The application port is published only on loopback; Caddy publishes HTTP/HTTPS and reaches `app:8080` on the Compose network. `deploy/Caddyfile` reads `PHENOGRAM_LANDING_DOMAIN` (default `http://localhost`), `PHENOGRAM_APP_DOMAIN` (default `http://app.localhost`), and `PHENOGRAM_API_DOMAIN` (default `http://api.localhost`) and enforces the same three-host split as production. Direct local access to port 8080 remains a supported one-origin development mode when all three base URLs are `http://localhost:8080`; in that mode start only `postgres` and `app`, not Caddy. `compose.premium.yaml` is an optional overlay for the separate official Telegram service.

Application startup is fail-fast: configuration is validated, PostgreSQL is connected, migrations are applied, background tasks are started, and only then is the HTTP listener bound. `SIGTERM` and `SIGINT` stop the HTTP server gracefully. A delivery interrupted after the lease is taken is marked retryable by the retention task once its five-minute lease expires.

## Configuration

| Variable | Required/default | Operational meaning |
| --- | --- | --- |
| `APP_ENV` | `development` | Set exactly to `production` for HTTPS/config checks and secure cookies. |
| `LISTEN_ADDR` | `127.0.0.1:8080` | Compose overrides this with `0.0.0.0:8080`. |
| `LANDING_BASE_URL` | Required | Canonical public landing origin without a trailing slash. Production value: `https://phenogram.io`. |
| `APP_BASE_URL` | Required | Canonical authenticated console and management origin without a trailing slash. Browser `Origin` must match it exactly; health is served here. Production value: `https://app.phenogram.io`. |
| `API_BASE_URL` | Required | Canonical machine origin without a trailing slash. Used for Telegram ingress, Telegram-compatible bot/file paths, SSE, and signed file URLs. Production value: `https://api.phenogram.io`. |
| `DATABASE_URL` | Required | PostgreSQL connection URL. The pool uses 2–20 connections per app instance. |
| `MASTER_KEY` | Required, at least 32 bytes | Encrypts bot tokens and webhook secrets after domain-separated derivation. |
| `PUBLIC_ID_KEY` | Required, at least 32 bytes | Derives stable bot public IDs used for token lookup and public routes. |
| `LINK_SIGNING_KEY` | Required, at least 32 bytes | Signs public files and derives CSRF tokens. |
| `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET` | Required in production | Confidential Google Web OAuth client used only for sign-in. Its callback is derived from `APP_BASE_URL`. |
| `GITHUB_OAUTH_CLIENT_ID`, `GITHUB_OAUTH_CLIENT_SECRET` | Required in production | Dedicated GitHub OAuth App used only for sign-in. Its callback is derived from `APP_BASE_URL`. |
| `TELEGRAM_CLOUD_API_URL` | `https://api.telegram.org` | Cloud Bot API origin; useful to replace only in isolated tests. |
| `TELEGRAM_LOCAL_API_URL` | Unset | Separate local Bot API origin. Local routing is unavailable when unset. |
| `TELEGRAM_LOCAL_DATA_DIR` | Unset | Absolute read-only root containing local Bot API files. Required to serve opaque absolute `file_path` references. |
| `SESSION_TTL_HOURS` | `720` | New session lifetime. |
| `RETENTION_SWEEP_SECONDS` | `3600` | Interval between sweeps. Do not set to zero. |
| `RUST_LOG` | application info | Structured stdout log filter. |
| `POSTGRES_PASSWORD` | development default in Compose | Builds `DATABASE_URL` in the base stack. Never use the default in production. |
| `PHENOGRAM_LANDING_DOMAIN` | `http://localhost` in Compose | Caddy landing site address. Production value: `phenogram.io`. |
| `PHENOGRAM_APP_DOMAIN` | `http://app.localhost` in Compose | Caddy console/management site address. Production value: `app.phenogram.io`. |
| `PHENOGRAM_API_DOMAIN` | `http://api.localhost` in Compose | Caddy machine API site address. Production value: `api.phenogram.io`. |
| `PHENOGRAM_HTTP_PORT`, `PHENOGRAM_HTTPS_PORT` | `80`, `443` | Published Caddy ports. |
| `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | Premium overlay only | Operator-owned Telegram application credentials for the official local server. |

Generate the three application keys independently. Store them, database credentials, and OAuth client secrets in the deployment secret manager, not in the image, repository, Compose manifest, shell history, or centralized logs. Production validation rejects short application secrets and values containing `development`, but it cannot assess entropy. OAuth client IDs are not confidential by protocol, but the production workflow keeps each complete provider credential pair together in repository secrets.

## Social OAuth provider setup

Phenogram uses both providers only to authenticate a person. It stores the provider name, stable provider subject ID, display name/handle, and avatar URL required for the console. It does not request, receive, or store an email address or persist a provider token. The callback uses an access token transiently to resolve identity and then drops it. Google requests only `openid profile`; GitHub requests no OAuth scope and uses a field-selective GraphQL query for `databaseId`, `login`, `name`, and `avatarUrl`. Never add an email scope or field or call GitHub's email endpoint.

Configure a dedicated production Google Cloud project and Web application OAuth client according to Google's [web-server OAuth guide](https://developers.google.com/identity/protocols/oauth2/web-server), [OpenID Connect reference](https://developers.google.com/identity/openid-connect/openid-connect), and [production branding requirements](https://support.google.com/cloud/answer/15549049):

1. Set the audience to **External** and publish the app **In production** when it is ready for public users. Google Testing mode is limited to configured test users and their authorizations expire after seven days.
2. Configure the public app homepage as `https://phenogram.io`, register `phenogram.io` as an authorized domain, verify ownership through Google Search Console, and use `https://phenogram.io/privacy` as the public privacy-policy URL on both the landing page and OAuth branding screen. Google requires operator support/developer contact details in its console; Phenogram does not ingest those addresses.
3. Create a **Web application** OAuth client. Set the only production authorized redirect URI used by Phenogram to exactly `https://app.phenogram.io/api/auth/oauth/google/callback`, including scheme, host, path, case, and lack of trailing slash. No authorized JavaScript origin is required because this is a server-side authorization-code flow.
4. Request only `openid profile`. The OIDC `email` scope is optional and is deliberately omitted. Do not enable unrelated Google API scopes.
5. Save the client ID and secret as GitHub repository secrets `GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET`.

Create a dedicated OAuth App under a maintained GitHub account, following GitHub's [OAuth App registration](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/creating-an-oauth-app) and [web authorization flow](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps). GitHub permits either personal- or organization-owned OAuth Apps; prefer organization ownership when it is available, but do not block production on ownership transfer:

1. Use **Phenogram Platform** as the application name and `https://phenogram.io` as the Homepage URL.
2. Set the Authorization callback URL to exactly `https://app.phenogram.io/api/auth/oauth/github/callback`. A GitHub OAuth App has one configured callback URL, so use a separate app for development. Leave Device Flow disabled.
3. Keep this app dedicated to sign-in and request no OAuth scope. In particular, never request `user` or `user:email`; GitHub documents that an empty scope grants read-only access to public identity. If these credentials were previously used to request broader scopes, replace the OAuth App rather than relying on a later empty-scope request, because GitHub can reuse grants previously authorized for the same app.
4. Save the client ID and secret as GitHub repository secrets `PHENOGRAM_GITHUB_OAUTH_CLIENT_ID` and `PHENOGRAM_GITHUB_OAUTH_CLIENT_SECRET`. GitHub reserves repository-secret names beginning with `GITHUB_`; the workflow maps these names to the application's `GITHUB_OAUTH_CLIENT_ID` and `GITHUB_OAUTH_CLIENT_SECRET` environment variables.

For local development, create separate provider clients and use callbacks derived exactly from the local `APP_BASE_URL`. The default split-host callbacks are `http://app.localhost/api/auth/oauth/google/callback` and `http://app.localhost/api/auth/oauth/github/callback`. If a provider console refuses the `app.localhost` form, use the documented one-origin mode with all three base URLs set to `http://localhost:8080`, register `http://localhost:8080/api/auth/oauth/{provider}/callback`, and start only `postgres` and `app`. Do not reuse the production GitHub OAuth App because it supports only one callback.

## Initial production deployment

1. Provision a supported Docker host or equivalent orchestrator, durable PostgreSQL storage, encrypted backups, DNS, and inbound HTTPS.
2. Copy `.env.example` to an untracked deployment secret file. Set `APP_ENV=production`, `LANDING_BASE_URL=https://phenogram.io`, `APP_BASE_URL=https://app.phenogram.io`, `API_BASE_URL=https://api.phenogram.io`, a strong database password, three independent application keys, and both provider credential pairs. Complete the provider setup above before starting the service; production starts fail-fast if either pair is missing.
3. Set `PHENOGRAM_LANDING_DOMAIN=phenogram.io`, `PHENOGRAM_APP_DOMAIN=app.phenogram.io`, and `PHENOGRAM_API_DOMAIN=api.phenogram.io`, then expose the included Caddy service, or place an equivalent reverse proxy in front of `app:8080`. Preserve SSE streaming. The landing host may proxy only `/`, `/privacy`, `/assets/app.css`, `/assets/app.js`, and `/assets/runtime.js`; the app host must reject machine routes; the API host must accept only `/bot*`, `/file[/...]`, `/telegram[/...]`, `/events[/...]`, and `/public[/...]`. The supplied Caddyfile enforces that split, enables compression and security headers, replaces the complete URI in access logs, and deletes cookie, authorization, Telegram secret, and CSRF headers from access logs.
4. Restrict direct access to port 8080 and PostgreSQL. Only the reverse proxy should reach the app; only the app and authorized operators should reach the database. The ingress must overwrite, not trust, client-supplied `X-Forwarded-For` and `X-Real-IP`; the bundled Caddyfile does this.
5. Back up PostgreSQL before every upgrade that contains migrations.
6. Build and start the base stack:

   ```sh
   docker compose up --build -d
   docker compose ps
   curl -fsS https://app.phenogram.io/api/health
   ```

7. Confirm the response is HTTP 200 with `status: "ok"` and `database: true`. Confirm the landing page loads at `https://phenogram.io/`, `https://phenogram.io/api/health` returns 404, a machine route on `https://app.phenogram.io` returns 404, and both `https://api.phenogram.io/` and `https://api.phenogram.io/api/health` return 404. Confirm each OAuth start endpoint redirects only to its expected provider, complete one real sign-in with each provider, connect a test bot, make a proxied `getMe` call through `api.phenogram.io`, deliver a real Telegram update, and confirm it appears in the update journal.

The health endpoint checks PostgreSQL only. It does not prove reachability of Telegram, the downstream webhook, SSE fan-out, the optional local server, or disk headroom; keep those as separate checks.

Migrations run automatically on application startup. There is no automated schema downgrade. Review migrations and take a restorable backup before deploying a new binary.

## TLS and log handling

The token-bearing compatibility route intentionally matches Telegram:

```text
/bot{token}/{method}
/file/bot{token}/{file_path}
```

Public SSE URLs contain a stream secret, signed file URLs contain a reusable signature until expiry, and OAuth callbacks contain short-lived authorization codes and state. Redact the complete request target—including query parameters—at every hop. The included Caddyfile replaces `request.uri` with `/redacted` and drops `Cookie`, `Authorization`, `X-Telegram-Bot-Api-Secret-Token`, and `X-Phenogram-CSRF`. If a CDN, ingress controller, APM agent, packet capture, or upstream application-error logger is added, verify its behavior with synthetic credentials before production use.

Keep all three public DNS names as direct, DNS-only records when using Cloudflare in front of this deployment. A proxy/CDN adds another request-log and buffering layer in front of token-bearing Bot API paths, signed URLs, and SSE. If an edge proxy is introduced later, first prove full-URI redaction, disabled buffering for SSE, large upload/download behavior, and forwarding-header overwrites with synthetic credentials.

Do not log request bodies: updates and Bot API requests may contain personal data. Limit access to application logs because network errors can include destination context even when proxy access logs are scrubbed.

OAuth state cookies are short-lived and are validated before exchanging a callback code. Add distributed edge limits to OAuth start and callback routes before running multiple replicas or accepting substantial public traffic; provider-side limits are not a substitute for application-edge abuse controls.

When a bot already has a Telegram webhook, connection automatically imports its URL, `allowed_updates`, and `max_connections` into `bot_update_state` before Phenogram installs its managed Telegram ingress. This lets Phenogram journal each update and continue delivery to the existing destination without a confirmation/retry step. The downstream URL is retained because it is required for delivery; the expiring audit entry records only whether a transfer occurred. Migration `0006_remove_plaintext_webhook_url.sql` removes the obsolete duplicate URL column from `bots`.

Telegram's `getWebhookInfo` response does not include the existing `secret_token` or uploaded certificate. If the downstream endpoint validates `X-Telegram-Bot-Api-Secret-Token`, call `setWebhook` through Phenogram after connecting to store that secret. A webhook using a custom certificate cannot be transferred automatically and must first move to a publicly trusted HTTPS certificate. Treat downstream webhook URLs as sensitive operational data in database access, exports, and backups.

## Monitoring and readiness

Minimum alerts:

- `https://app.phenogram.io/api/health` is non-200 or reports `database: false`;
- application or PostgreSQL container is unhealthy/restarting;
- sustained HTTP 5xx/502 responses;
- elevated OAuth start/callback errors, state mismatches, provider timeouts, or provider-side throttling;
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
3. deployment configuration, OAuth client configuration, and image/source revision metadata;
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

Test restores into an isolated PostgreSQL instance with no production traffic. Verify migrations, row counts, a provider sign-in using non-production OAuth clients, decryption through a non-production bot, update history, and signed-link behavior using the backed-up key versions. Never use `--clean` against the live database as a casual restore test.

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

### OAuth client credentials

Existing Phenogram sessions do not contain or depend on provider access tokens, so rotating a provider client secret affects only new sign-ins and callbacks currently in flight.

For GitHub, generate a new client secret on the dedicated OAuth App, update repository secret `PHENOGRAM_GITHUB_OAUTH_CLIENT_SECRET`, deploy, verify a fresh sign-in, and then delete the old secret. GitHub recommends exactly that order after a secret compromise. Do not change the app's no-scope policy during rotation.

For Google, create a replacement Web application client with the same production callback, update both `GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET`, deploy, verify a fresh sign-in, and then delete the old client. This permits controlled rollback, unlike resetting the existing client's secret. An authorization flow started immediately before the deployment may need to be restarted. Keep `openid profile` as the complete scope set.

In both cases, use GitHub repository secrets and trigger the `master` production workflow. It reconciles the values directly into `phenogram-secrets`; the values are never Helm parameters or Helm release history. Review callback failure rates and remove superseded credentials after verification.

### Master encryption key

Do not simply change `MASTER_KEY`: the application has no online re-encryption command, and existing bot tokens, ingress/downstream secrets, and outstanding opaque local-file references would become unreadable. A safe rotation requires a purpose-built decrypt/re-encrypt migration while the old key is available, or controlled deletion and reconnection of every bot. Back up first and verify a restore.

### Public-ID key

Do not rotate `PUBLIC_ID_KEY` in place. Proxy authentication derives a public ID from the presented bot token and looks up the stored ID; a new key breaks every bot route and leaves Telegram pointing at old ingress URLs. Rotation requires a coordinated database/public-URL migration and upstream webhook reprovisioning, which this MVP does not automate.

### Bot tokens

Revoke a suspected token with @BotFather immediately. This MVP has no in-place token replacement endpoint. Deleting and reconnecting the bot removes its Phenogram history through cascading deletes, so preserve required evidence under the applicable policy before that destructive step.

### Database and local-server credentials

Rotate the PostgreSQL role secret together with `DATABASE_URL` and restart the app. For `TELEGRAM_API_ID`/`TELEGRAM_API_HASH`, follow Telegram's credential process, update the companion service secret, and test bot login and update ingress before routing traffic.

## Plan administration and retention

A user's first provider sign-in assigns the `free` plan. There is no payment provider or admin endpoint in this MVP; the plan-selection UI is informational. Until billing exists, authorized operators assign plans directly in PostgreSQL and should record the approval externally and in the audit log.

Inspect before changing a membership:

```sql
SELECT users.id, identities.provider, identities.provider_login,
       memberships.plan_id, memberships.status,
       plans.bot_limit, plans.retention_days, plans.local_bot_api
FROM users
JOIN oauth_identities identities ON identities.user_id = users.id
JOIN memberships ON memberships.user_id = users.id
JOIN plan_definitions plans ON plans.id = memberships.plan_id
WHERE identities.provider = 'github'
  AND identities.provider_login = 'owner-handle';
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

1. Stop rollouts and confirm whether `https://app.phenogram.io/api/health` is 503.
2. Inspect container health, PostgreSQL logs, storage capacity/inodes, connection count, and host pressure.
3. Preserve logs and take a snapshot before repairing suspected corruption.
4. Restore connectivity or fail over PostgreSQL, then verify migrations and `https://app.phenogram.io/api/health`.
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

- `phenogram.io` is the public landing surface and exposes only `/`, `/privacy`, `/assets/app.css`, `/assets/app.js`, and `/assets/runtime.js`. `app.phenogram.io` owns the authenticated console, client routes, management API, and health. `api.phenogram.io` is restricted to Telegram-compatible bot/file paths, Telegram ingress, SSE, and signed public downloads; the landing and API hosts intentionally do not expose `/api/health` or management endpoints.
- SSE is the only new subscription transport. Kafka, WebSockets, NATS, and similar brokers are not implemented.
- Live SSE fan-out and its 256-global/four-per-key connection caps are process-local. Replay is durable but capped per connection at 5,000 rows, 8 MiB total, and roughly 2 MiB per event.
- Downstream delivery uses four workers per app process. Stored `max_connections` is informational in this MVP.
- Payment collection, provider reconciliation, tax/invoice handling, self-service plan changes, and an admin console are absent.
- Plan retention covers updates, outbound messages, API calls, conversation projections, and bot-scoped audit rows, but changing a plan does not recalculate already stamped expiry times. Account-scoped audit rows use one year.
- The optional official local Bot API server is a separate operational component. Routing, managed webhook reprovisioning, opaque local paths, and range delivery are integrated, but the platform is not a local-server monitoring or backup control plane.
- Bot credential replacement is absent. Reconnecting after a BotFather token rotation requires deleting the existing bot record, which cascades its stored history.
- Authentication depends on Google and GitHub availability and has no MFA policy of its own, organization model, or RBAC. Phenogram requests no email scope or field and receives or stores neither email addresses nor provider tokens. OAuth endpoints still need distributed edge limits before horizontal scaling.
- Downstream destination pinning and global-address checks reduce SSRF risk, but operator-level egress controls remain required defense in depth.
- The Helm deployment includes scheduled encrypted PostgreSQL backups to an operator-provisioned S3-compatible bucket, but no built-in metrics endpoint, point-in-time recovery, multi-region failover, or automated disaster recovery.
- Compatibility has been exercised for the included contract tests, not certified against every Telegram method/content type. Managed update methods intentionally implement a subset of Telegram parameters.
- Managed `setWebhook` does not accept multipart custom certificates or implement `ip_address`; `max_connections` is stored/reported but does not control the global four-worker pool.

These boundaries make the safest initial shape a protected, single-instance deployment with managed PostgreSQL/backups, strict ingress redaction and rate controls, a small authorized user base, and explicit operator ownership of plans and premium routing.
