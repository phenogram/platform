# Phenogram production chart

This chart deploys one Phenogram application pod, a persistent PostgreSQL 17
database, encrypted logical backups, ingress-nginx, and the official
Telegram Bot API server as an application sidecar. It targets the `phenogram`
namespace on Contabo and publishes three deliberately separate origins:

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
- ingress-nginx in `ingress-nginx`, labeled
  `app.kubernetes.io/name=ingress-nginx`.
- cert-manager with a `letsencrypt-prod` ClusterIssuer.
- The `standard` StorageClass.
- Flux source-controller and helm-controller with support for OCIRepository and
  HelmRelease resources. Apply `deploy/flux/phenogram-production.yaml` once as
  a cluster administrator.
- DNS-only `A` records for `phenogram.io`, `app.phenogram.io`, and
  `api.phenogram.io`, each pointing to `84.247.177.201`.

Do not enable Cloudflare proxying for these hostnames. Bot API file transfers
and SSE connections should terminate directly at ingress-nginx, and token-bearing
machine API paths must not be recorded by URI access logs. Configure ingress-nginx
access-log redaction or keep access logging disabled before production traffic
arrives.

## Secret contract

The chart deliberately renders no Kubernetes Secret. Provision these three
external resources in the `phenogram` namespace before enabling the
HelmRelease:

- `phenogram-ghcr`, a `kubernetes.io/dockerconfigjson` credential that can read
  the private image and chart packages under `ghcr.io/phenogram`.
- `phenogram-secrets`, with `DATABASE_URL`, `POSTGRES_PASSWORD`, `MASTER_KEY`,
  `PUBLIC_ID_KEY`, `LINK_SIGNING_KEY`, `GOOGLE_OAUTH_CLIENT_ID`,
  `GOOGLE_OAUTH_CLIENT_SECRET`, `GITHUB_OAUTH_CLIENT_ID`,
  `GITHUB_OAUTH_CLIENT_SECRET`, `TELEGRAM_API_ID`, and `TELEGRAM_API_HASH`.
- `phenogram-backup-secrets`, with `access-key`, `secret-key`, `endpoint`,
  `bucket`, and `restic-password`.

`DATABASE_URL` must address
`postgresql://phenogram:<encoded-password>@phenogram-postgresql:5432/phenogram`.
The three application keys are independent random values of at least 32 bytes.
The OAuth credentials belong to dedicated sign-in-only provider applications:
Google's callback is
`https://app.phenogram.io/api/auth/oauth/google/callback` with only
`openid profile`, and GitHub's callback is
`https://app.phenogram.io/api/auth/oauth/github/callback` with no OAuth scope.
Phenogram stores neither email addresses nor provider tokens. Secret values are
never passed as Helm values, so they remain absent from Helm release history.
Manage them through a restricted cluster secret workflow or SOPS. After a
manual secret rotation, restart the application Deployment so its processes
load the new values.

GitHub Actions needs no Kubernetes, SSH, runtime, database, OAuth, or Telegram
credential. A push to `master` uses only the ephemeral repository
`GITHUB_TOKEN` to publish digest-pinned images and a merged production Helm
chart to `ghcr.io/phenogram/phenogram-platform`. Flux selects the newest chart
version and helm-controller performs the namespace-scoped upgrade on Contabo.

The chart renders namespace-scoped NetworkPolicy resources. Contabo's current
Minikube `bridge` CNI does not enforce NetworkPolicy, so these manifests are
desired state rather than active isolation until a policy-capable CNI is
installed.

## Storage and backups

Production requests 10 GiB for PostgreSQL and 20 GiB for Telegram local files;
both PVCs carry Helm's `keep` policy. Contabo's `standard` StorageClass cannot
expand an existing claim, so increasing either size requires a controlled data
move to a new PVC. Every six hours the backup CronJob creates
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
digests, publishes an immutable OCI chart, waits for Flux to expose the exact
commit revision on the Contabo origin, and runs positive and negative checks
across all three hosts. Take a restorable backup before merging a migration
into `master` when production contains data worth preserving.
