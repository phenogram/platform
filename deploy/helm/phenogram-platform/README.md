# Phenogram production chart

This chart deploys one Phenogram application pod, a persistent PostgreSQL 17
database, encrypted off-site logical backups, Traefik ingress, and the official
Telegram Bot API server as an application sidecar. It targets the `phenogram`
namespace on Rubase and publishes three deliberately separate origins:

- `https://phenogram.io` serves only the public landing experience and the
  crawlable `https://phenogram.io/privacy` policy required for provider
  branding.
- `https://app.phenogram.io` serves the authenticated developer console, its
  assets, and same-origin management `/api/*` routes.
- `https://api.phenogram.io` serves only Telegram-compatible `/bot*`, `/file/*`,
  `/telegram/*`, `/events/*`, and `/public/*` machine routes.

The local Bot API server shares a `ReadWriteOnce` file volume with the Rust
application, which must read premium files directly. The production Deployment
therefore uses one replica and a `Recreate` strategy. Horizontal scaling needs a
different shared-file design plus distributed SSE and rate limiting.

## Cluster prerequisites

- An existing `phenogram` namespace and the namespace-only deployment service
  account defined in `deploy/bootstrap/github-deployer.yaml`.
- Traefik in `kube-system`, labeled `app.kubernetes.io/name=traefik`.
- cert-manager with a `letsencrypt-prod` ClusterIssuer.
- The default `local-path` StorageClass.
- DNS-only `A` records for `phenogram.io`, `app.phenogram.io`, and
  `api.phenogram.io`, each pointing to `185.221.212.224`.

Do not enable Cloudflare proxying for these hostnames. Bot API file transfers
and SSE connections should terminate directly at Traefik, and token-bearing
machine API paths must not be recorded by URI access logs. Configure Traefik
access-log redaction or keep access logging disabled before production traffic
arrives.

## Secret contract

The chart deliberately renders no Kubernetes Secret. The deployment workflow
reconciles these three external resources before Helm runs:

- `phenogram-ghcr`, from repository secret
  `PHENOGRAM_GHCR_PULL_CONFIG_JSON`.
- `phenogram-secrets`, from repository secrets `DATABASE_URL`,
  `POSTGRES_PASSWORD`, `MASTER_KEY`, `PUBLIC_ID_KEY`, `LINK_SIGNING_KEY`,
  `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`,
  `PHENOGRAM_GITHUB_OAUTH_CLIENT_ID`,
  `PHENOGRAM_GITHUB_OAUTH_CLIENT_SECRET`, `TELEGRAM_API_ID`,
  and `TELEGRAM_API_HASH`.
- `phenogram-backup-secrets`, from the five
  `PHENOGRAM_BACKUP_*` repository secrets.

`DATABASE_URL` must address
`postgresql://phenogram:<encoded-password>@phenogram-postgresql:5432/phenogram`.
The three application keys are independent random values of at least 32 bytes.
The OAuth credentials belong to dedicated sign-in-only provider applications:
Google's callback is
`https://app.phenogram.io/api/auth/oauth/google/callback` with only
`openid profile`, and GitHub's callback is
`https://app.phenogram.io/api/auth/oauth/github/callback` with no OAuth scope.
Phenogram stores neither email addresses nor provider tokens.
The deploy credential itself is stored as `RUBASE_PHENOGRAM_KUBECONFIG` and
must contain a kubeconfig whose current context is fixed to `phenogram`.

Secrets are sent directly to the Kubernetes API and never passed as Helm values,
so they are absent from Helm release history. GitHub logs do not deliberately
print them; keep secret masking enabled and restrict the `production`
environment to trusted maintainers.

The workflow passes only the Kubernetes Secret's non-sensitive resource version
to Helm. A secret change therefore updates the pod-template annotation and
restarts the consumers even when a manual deployment reuses the same commit.

## Storage and backups

Production requests 10 GiB for PostgreSQL and 20 GiB for Telegram local files;
both PVCs carry Helm's `keep` policy. Every six hours the backup CronJob creates
a compressed custom-format `pg_dump`, validates it with `pg_restore --list`,
uploads it into a dedicated encrypted Restic repository on Contabo MinIO,
applies 14 daily/8 weekly/12 monthly retention, and checks 5% of repository data.

Provision the limited bucket user once with
`deploy/bootstrap/contabo-minio-backup.yaml`. Perform an isolated restore drill
after provisioning and after material schema or backup-tool changes.

## Validation

```sh
helm lint deploy/helm/phenogram-platform \
  --values deploy/helm/phenogram-platform/values-production.yaml

helm template phenogram deploy/helm/phenogram-platform \
  --namespace phenogram \
  --values deploy/helm/phenogram-platform/values-production.yaml \
  --set-string image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set-string telegramBotApi.image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
```

Migrations run during application startup. The workflow uses immutable image
digests, an atomic Helm upgrade, rollout checks, a database-backed health check
on the app origin, and negative checks that the landing and machine origins do
not expose management health. Take a restorable backup before merging a
migration into `master`.
