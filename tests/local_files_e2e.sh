#!/usr/bin/env bash
set -euo pipefail

base_url="${PHENOGRAM_E2E_URL:-http://127.0.0.1:18080}"
database_url="${PHENOGRAM_E2E_DATABASE_URL:?Set PHENOGRAM_E2E_DATABASE_URL}"
cookie_jar="$(mktemp /private/tmp/phenogram-local-e2e-cookies.XXXXXX)"
trap 'rm -f "$cookie_jar"' EXIT

email="e2e-local-$(date +%s)@example.test"
password="correct-horse-battery-staple"
bot_token="123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"

psql "$database_url" -v ON_ERROR_STOP=1 \
  -c "DELETE FROM users WHERE email LIKE 'e2e-%@example.test' OR email = 'browser-final@example.test'" >/dev/null

session="$(curl -fsS -c "$cookie_jar" \
  -H 'content-type: application/json' \
  -d "{\"email\":\"$email\",\"password\":\"$password\"}" \
  "$base_url/api/auth/register")"
csrf="$(jq -er '.csrf_token' <<<"$session")"
connected="$(curl -fsS -b "$cookie_jar" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"token\":\"$bot_token\",\"accept_webhook_takeover\":true}" \
  "$base_url/api/bots")"
bot_id="$(jq -er '.bot.id' <<<"$connected")"

psql "$database_url" -v ON_ERROR_STOP=1 \
  -c "UPDATE memberships SET plan_id = 'pro', status = 'active' WHERE user_id = (SELECT id FROM users WHERE email = '$email')" >/dev/null

routed="$(curl -fsS -b "$cookie_jar" \
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

cloud="$(curl -fsS -b "$cookie_jar" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d '{"mode":"cloud","confirm_migration":true}' \
  "$base_url/api/bots/$bot_id/routing")"
jq -e '.bot.routing_mode == "cloud" and .bot.status == "healthy"' <<<"$cloud" >/dev/null
curl -fsS "${PHENOGRAM_MOCK_URL:-http://127.0.0.1:18081}/__state" \
  | jq -e '.webhook.url | contains("/telegram/webhook/phg_")' >/dev/null

printf 'Phenogram premium E2E passed: both routing migrations, webhook provisioning, opaque local path rewrite, and streamed download.\n'
