# Phenogram production chart

This chart deploys Phenogram directly to the `phenogram` namespace on Contabo with Helm. Application delivery is owned by this repository's `master` GitHub Actions workflow; it does not use Flux or `infra-flux`.

It publishes three separate origins:

- `https://phenogram.io` — landing page and privacy policy;
- `https://app.phenogram.io` — authenticated console and management `/api/*`;
- `https://api.phenogram.io` — Telegram-compatible `/bot*` and `/file*`, plus `/telegram/*`, `/events/*`, and `/public/*` machine routes.

## Official data plane

With `dataPlane.enabled=true`, the chart creates:

- one streaming gateway with a durable last-good snapshot of HMAC-derived token lookups;
- one replica-one pinned official `telegram-bot-api` StatefulSet for the `standard` pool;
- one replica-one pinned official `telegram-bot-api --local` StatefulSet for the `local` pool;
- a nonblocking tap collector beside each official server;
- an authenticated read-only file sidecar beside each official server.

The gateway parses only the token-bearing route structure needed to authenticate and select a pool. It does not query PostgreSQL, parse Bot API arguments, buffer bodies, or retry requests. It streams the raw request and response through the pinned official server, which remains the sole owner of `TQueue`, `getUpdates`, webhooks, retries, and acknowledgement behavior.

The official server has no public HTTP `/file` endpoint. The gateway therefore sends file requests over an authenticated internal hop to the sidecar, which reads the same pool PVC read-only and supports `GET`, `HEAD`, and ranges.

The official patch emits ordinary update mirrors only after the native queue/wakeup path; those type-1 datagrams remain best-effort and may be dropped without affecting native delivery. Managed-bot lifecycle signals use a separate bounded persistent observer queue and replay until the collector commits an idempotent PostgreSQL receipt and returns an exact ACK. Its 10,000-record/seven-day cap still fails open on overflow, expiry, or persistence failure. Neither collector nor file-sidecar health is part of the official service endpoint readiness.

## Cluster prerequisites

- Existing `phenogram` namespace and the namespace-only service account from `deploy/bootstrap/github-deployer.yaml`.
- ingress-nginx in `ingress-nginx`, labeled `app.kubernetes.io/name=ingress-nginx`.
- ingress-nginx configured to allow the chart's Critical-risk server snippet. The API host uses `error_log /dev/null crit` because raw bot tokens are in the request path.
- cert-manager and the `letsencrypt-prod` ClusterIssuer.
- `standard` StorageClass.
- Repository secret `KUBECONFIG`, restricted to the `phenogram` namespace and the Contabo API endpoint.
- DNS-only Cloudflare `A` records for `phenogram.io`, `app.phenogram.io`, and `api.phenogram.io`, all pointing to `84.247.177.201`.

Do not enable Cloudflare proxying for these hosts. Bot API uploads/downloads, long polling, and SSE terminate directly at ingress-nginx. The chart disables API-host access logs, discards its nginx error log, disables request/response buffering, preserves double slashes, and removes the aggregate request-body cap. Re-verify token-marker absence after every ingress-controller change.

## Secret contract

The chart renders no Kubernetes Secret. Provision these resources before Helm runs:

- `phenogram-ghcr`, a `kubernetes.io/dockerconfigjson` credential for private `ghcr.io/phenogram` images;
- `phenogram-secrets`, containing `DATABASE_URL`, `POSTGRES_PASSWORD`, `MASTER_KEY`, `PUBLIC_ID_KEY`, `LINK_SIGNING_KEY`, `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `GITHUB_OAUTH_CLIENT_ID`, `GITHUB_OAUTH_CLIENT_SECRET`, `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, and, when the official data plane is enabled, `DATA_PLANE_SYNC_TOKEN`;
- `phenogram-backup-secrets`, containing the currently enabled PostgreSQL Restic target keys;
- the three cert-manager TLS Secrets.

`MASTER_KEY`, `PUBLIC_ID_KEY`, `LINK_SIGNING_KEY`, and `DATA_PLANE_SYNC_TOKEN` are independent values of at least 32 bytes. OAuth applications are sign-in-only: Google requests `openid profile`; GitHub requests no scope. Phenogram requests and stores no email addresses or provider tokens.

GitHub Actions receives only the namespace-limited `KUBECONFIG` and repository secret `DATA_PLANE_SYNC_TOKEN`. The workflow reconciles that data-plane key directly into the existing Secret before Helm. It is never a Helm value or release-history entry. Other application, OAuth, Telegram, backup, and TLS credentials remain cluster-managed.

`DATA_PLANE_SYNC_TOKEN` authenticates route snapshots, gateway telemetry, and gateway-to-file-sidecar requests. This matters because Contabo's current Minikube bridge CNI does not enforce the rendered NetworkPolicy objects. Those objects are desired state, not a currently enforced security boundary.

## Native credential storage

Application bot credentials remain encrypted in PostgreSQL with `MASTER_KEY`. Official-server storage is intentionally different: each pool uses the pinned upstream native format unchanged. Its PVC may contain complete bot tokens in directory names and persisted runtime state, including `<raw-token>/` and test-DC `<raw-token>:T/` directories.

There is no Phenogram official-storage encryption, KDF, hashed token-directory layer, custom TDLib database key, or encrypted envelope. Treat node, PVC, CSI snapshot, and volume-copy access as bot-token access. The file sidecar shares the PVC read-only; the official server is its only writer.

## Immutable image pins

The data plane refuses to render unless these four images have independent immutable digests:

- `dataPlane.gateway.image.digest`;
- `dataPlane.official.image.digest`;
- `dataPlane.official.collector.image.digest`;
- `dataPlane.official.fileServer.image.digest`.

The collector uses the application binary and the file sidecar uses the gateway binary, but their pins are deliberately advanced separately. A normal application or gateway release must not restart the official pool. Updating a collector or file-sidecar pin does restart the shared pod and is therefore a deliberate maintenance event.

## Destructive first activation

This release does not migrate legacy bots or copy the old Telegram data directory. Before the first release, delete the disposable legacy bot rows and their cascaded bot data; preserve account identities and browser sessions.

1. **Build release:** keep `dataPlane.enabled=false`, `dataPlane.publicCutover=false`, and `telegramBotApi.enabled=true`. Push to `master` and record the immutable application, official, and gateway digests produced by GitHub Actions.
2. **Shadow release:** pin the four data-plane digests, set `dataPlane.enabled=true`, keep `dataPlane.publicCutover=false`, and leave `telegramBotApi.enabled=true`. Reconcile `DATA_PLANE_SYNC_TOKEN`. Verify the gateway route snapshot, both official pools, test DC, multipart/large streaming, file ranges, tap isolation, journal/SSE, and managed lifecycle behavior with disposable canaries. Do not copy a legacy bot session into either pool.
3. **Public cutover:** keep the same data-plane pins, set `dataPlane.publicCutover=true`, and set `telegramBotApi.enabled=false`. Helm rejects any public cutover with the legacy sidecar still enabled. Verify the Contabo origin, then permanently delete the obsolete legacy Telegram PVC.

Only `/bot*` and `/file*` move from the application ingress backend to the gateway. `/telegram/*`, `/events/*`, and `/public/*` remain on the control application. The shadow phase warms and verifies the replacement; it is not a migration, backup, fallback, or compatibility phase.

## Storage

Production requests:

- PostgreSQL: 10 GiB;
- gateway last-good route snapshot: 256 MiB;
- official standard pool: 20 GiB;
- official local pool: 100 GiB.

The data-plane PVCs carry Helm's `keep` policy. Contabo's current `standard` StorageClass cannot expand an existing claim. The first destructive release intentionally discards the old legacy Telegram PVC after cutover; it is not copied into the new pools.

The enabled backup CronJob covers PostgreSQL application state through a compressed dump and Restic. It does not back up, wrap, or encrypt the official Bot API PVCs and is not a prerequisite for the destructive empty-bot cutover.

## Validation

Base rendering:

```sh
helm lint deploy/helm/phenogram-platform \
  --values deploy/helm/phenogram-platform/values-production.yaml

helm template phenogram deploy/helm/phenogram-platform \
  --namespace phenogram \
  --values deploy/helm/phenogram-platform/values-production.yaml \
  --set-string image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set-string telegramBotApi.image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
```

Shadow rendering:

```sh
helm template phenogram deploy/helm/phenogram-platform \
  --namespace phenogram \
  --values deploy/helm/phenogram-platform/values-production.yaml \
  --set dataPlane.enabled=true \
  --set-string image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set-string telegramBotApi.image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --set-string dataPlane.gateway.image.digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc \
  --set-string dataPlane.official.image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --set-string dataPlane.official.collector.image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --set-string dataPlane.official.fileServer.image.digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
```

For public cutover add `--set dataPlane.publicCutover=true --set telegramBotApi.enabled=false`.

After every deployment, verify the exact commit revision through the Contabo origin, all workloads/PVCs/ingresses, gateway last-good readiness, official pool readiness, host separation, OAuth scopes, a native Bot API request, and collector-failure isolation. A green application health endpoint is not proof of native compatibility or observer completeness.
