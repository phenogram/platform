#!/usr/bin/env bash
set -euo pipefail

base_url="${PHENOGRAM_E2E_URL:-http://127.0.0.1:18080}"
database_url="${PHENOGRAM_E2E_DATABASE_URL:?Set PHENOGRAM_E2E_DATABASE_URL}"

identity_subject="e2e-local-$(date +%s)-$$"
session_token="$(openssl rand -hex 32)"
auth_cookie="phg_session=$session_token"
bot_token="123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"

cleanup() {
  psql "$database_url" -v ON_ERROR_STOP=1 -v subject="$identity_subject" <<'SQL' >/dev/null || true
DELETE FROM users
 WHERE id IN (
    SELECT user_id FROM oauth_identities
     WHERE provider = 'github' AND provider_subject = :'subject'
 );
SQL
}
trap cleanup EXIT

psql "$database_url" -v ON_ERROR_STOP=1 \
  -v subject="$identity_subject" -v session_token="$session_token" <<'SQL' >/dev/null
WITH new_user AS (
    INSERT INTO users DEFAULT VALUES RETURNING id
), new_identity AS (
    INSERT INTO oauth_identities
        (user_id, provider, provider_subject, display_name, provider_login)
    SELECT id, 'github', :'subject', 'Phenogram Local E2E', 'phenogram-local-e2e'
      FROM new_user
    RETURNING id, user_id
), new_membership AS (
    INSERT INTO memberships (user_id, plan_id, status)
    SELECT user_id, 'free', 'active' FROM new_identity
)
INSERT INTO sessions (user_id, identity_id, token_hash, expires_at)
SELECT user_id, id, digest(:'session_token', 'sha256'), now() + interval '1 hour'
  FROM new_identity;
SQL

session="$(curl -fsS -b "$auth_cookie" "$base_url/api/me")"
csrf="$(jq -er '.csrf_token' <<<"$session")"
connected="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"token\":\"$bot_token\",\"accept_webhook_takeover\":true}" \
  "$base_url/api/bots")"
bot_id="$(jq -er '.bot.id' <<<"$connected")"

psql "$database_url" -v ON_ERROR_STOP=1 -v subject="$identity_subject" <<'SQL' >/dev/null
UPDATE memberships
   SET plan_id = 'pro', status = 'active'
 WHERE user_id = (
    SELECT user_id FROM oauth_identities
     WHERE provider = 'github' AND provider_subject = :'subject'
 );
SQL

routed="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d '{"mode":"local","confirm_migration":true}' \
  "$base_url/api/bots/$bot_id/routing")"
jq -e '.bot.routing_mode == "local" and .bot.status == "healthy"' <<<"$routed" >/dev/null

file_info="$(curl -fsS \
  -H 'content-type: application/json' \
  -d '{"file_id":"premium-file"}' \
  "$base_url/bot$bot_token/getFile")"
file_path="$(jq -er '.result.file_path | select(startswith("__phenogram_local__/"))' <<<"$file_info")"
test "$(curl -fsS "$base_url/file/bot$bot_token/$file_path")" = "phenogram-premium-local-file"
test "$(curl -fsS -H 'range: bytes=0-8' "$base_url/file/bot$bot_token/$file_path")" = "phenogram"

cloud="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d '{"mode":"cloud","confirm_migration":true}' \
  "$base_url/api/bots/$bot_id/routing")"
jq -e '.bot.routing_mode == "cloud" and .bot.status == "healthy"' <<<"$cloud" >/dev/null
curl -fsS "${PHENOGRAM_MOCK_URL:-http://127.0.0.1:18081}/__state" \
  | jq -e '.webhook.url | contains("/telegram/webhook/phg_")' >/dev/null

printf 'Phenogram premium E2E passed: both routing migrations, webhook provisioning, opaque local path rewrite, and streamed download.\n'
