#!/usr/bin/env bash
set -euo pipefail

base_url="${PHENOGRAM_E2E_URL:-http://127.0.0.1:18080}"
mock_url="${PHENOGRAM_MOCK_URL:-http://127.0.0.1:18081}"
cookie_jar="$(mktemp /private/tmp/phenogram-e2e-cookies.XXXXXX)"
trap 'rm -f "$cookie_jar"' EXIT

email="e2e-$(date +%s)@example.test"
password="correct-horse-battery-staple"
bot_token="123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"

# Keep repeated local runs independent from prior mock deliveries/webhooks.
curl -fsS -X POST "$mock_url/__reset" | jq -e '.ok == true' >/dev/null

# The test bot is a singleton by design. Clear only prior disposable E2E owners.
if [[ -n "${PHENOGRAM_E2E_DATABASE_URL:-}" ]]; then
  psql "$PHENOGRAM_E2E_DATABASE_URL" -v ON_ERROR_STOP=1 \
    -c "DELETE FROM users WHERE email LIKE 'e2e-%@example.test'" >/dev/null
fi

health="$(curl -fsS "$base_url/api/health")"
jq -e '.status == "ok" and .database == true' <<<"$health" >/dev/null

session="$(curl -fsS -c "$cookie_jar" \
  -H 'content-type: application/json' \
  -d "{\"email\":\"$email\",\"password\":\"$password\"}" \
  "$base_url/api/auth/register")"
csrf="$(jq -er '.csrf_token' <<<"$session")"
jq -e '.membership.plan_id == "free" and .membership.bot_limit == 1' <<<"$session" >/dev/null

connected="$(curl -fsS -b "$cookie_jar" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"token\":\"$bot_token\",\"accept_webhook_takeover\":true}" \
  "$base_url/api/bots")"
bot_id="$(jq -er '.bot.id' <<<"$connected")"
public_id="$(jq -er '.bot.public_id' <<<"$connected")"
jq -e '.bot.username == "phenogram_test_bot" and (.bot | has("token_fingerprint") | not) and (.bot.public_id | length > 20)' <<<"$connected" >/dev/null

upstream_state="$(curl -fsS "$mock_url/__state")"
ingress_secret="$(jq -er '.webhook.secret_token' <<<"$upstream_state")"
jq -e --arg public_id "$public_id" '.webhook.url | endswith("/telegram/webhook/" + $public_id)' <<<"$upstream_state" >/dev/null

update='{"update_id":7001,"message":{"message_id":41,"date":1786620000,"from":{"id":99,"is_bot":false,"first_name":"Ada","username":"ada"},"chat":{"id":99,"type":"private","first_name":"Ada","username":"ada"},"text":"hello from the e2e test"}}'
curl -fsS \
  -H 'content-type: application/json' \
  -H "x-telegram-bot-api-secret-token: $ingress_secret" \
  -d "$update" \
  "$base_url/telegram/webhook/$public_id" | jq -e '.ok == true' >/dev/null

polled="$(curl -fsS \
  -H 'content-type: application/json' \
  -d '{"limit":10,"timeout":0}' \
  "$base_url/bot$bot_token/getUpdates")"
jq -e '.ok == true and .result[0].update_id == 7001' <<<"$polled" >/dev/null

# Positive offsets consume earlier polling updates.
curl -fsS \
  -H 'content-type: application/json' \
  -d '{"offset":7002,"limit":10,"timeout":0}' \
  "$base_url/bot$bot_token/getUpdates" | jq -e '.ok == true and .result == []' >/dev/null

# A successfully delivered downstream-webhook update must not replay when the
# developer later switches back to polling.
curl -fsS \
  -H 'content-type: application/json' \
  -d "{\"url\":\"$mock_url/__downstream\",\"allowed_updates\":[\"message\"]}" \
  "$base_url/bot$bot_token/setWebhook" | jq -e '.ok == true' >/dev/null
update_webhook='{"update_id":7002,"message":{"message_id":42,"date":1786620001,"from":{"id":99,"is_bot":false,"first_name":"Ada"},"chat":{"id":99,"type":"private","first_name":"Ada"},"text":"delivered once"}}'
curl -fsS \
  -H 'content-type: application/json' \
  -H "x-telegram-bot-api-secret-token: $ingress_secret" \
  -d "$update_webhook" \
  "$base_url/telegram/webhook/$public_id" | jq -e '.ok == true' >/dev/null
for _ in $(seq 1 40); do
  if curl -fsS "$mock_url/__state" | jq -e '.deliveries | any(.update_id == 7002)' >/dev/null; then
    break
  fi
  sleep 0.1
done
curl -fsS "$mock_url/__state" | jq -e '.deliveries | any(.update_id == 7002)' >/dev/null
curl -fsS \
  -H 'content-type: application/json' \
  -d '{"drop_pending_updates":false}' \
  "$base_url/bot$bot_token/deleteWebhook" | jq -e '.ok == true' >/dev/null
curl -fsS \
  -H 'content-type: application/json' \
  -d '{"limit":10,"timeout":0,"allowed_updates":["callback_query"]}' \
  "$base_url/bot$bot_token/getUpdates" | jq -e '.ok == true and (.result | map(.update_id) | index(7002) | not)' >/dev/null

# allowed_updates applies to future ingress, not retroactively to rows already
# eligible under the previous filter.
filtered_update='{"update_id":7003,"message":{"message_id":43,"date":1786620002,"from":{"id":99,"is_bot":false,"first_name":"Ada"},"chat":{"id":99,"type":"private","first_name":"Ada"},"text":"filtered from polling"}}'
curl -fsS \
  -H 'content-type: application/json' \
  -H "x-telegram-bot-api-secret-token: $ingress_secret" \
  -d "$filtered_update" \
  "$base_url/telegram/webhook/$public_id" | jq -e '.ok == true' >/dev/null
curl -fsS \
  -H 'content-type: application/json' \
  -d '{"limit":10,"timeout":0,"allowed_updates":["message"]}' \
  "$base_url/bot$bot_token/getUpdates" | jq -e '.ok == true and (.result | map(.update_id) | index(7003) | not)' >/dev/null

proxied="$(curl -fsS \
  -H 'content-type: application/json' \
  -d '{"chat_id":99,"text":"proxied response"}' \
  "$base_url/bot$bot_token/sendMessage")"
jq -e '.ok == true and .result.text == "proxied response"' <<<"$proxied" >/dev/null

operator_reply="$(curl -fsS -b "$cookie_jar" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d '{"chat_id":99,"text":"operator response"}' \
  "$base_url/api/bots/$bot_id/messages")"
jq -e '.ok == true and .result.text == "operator response"' <<<"$operator_reply" >/dev/null

updates="$(curl -fsS -b "$cookie_jar" "$base_url/api/bots/$bot_id/updates?limit=10")"
jq -e '.updates | any(.update_id == 7001 and .event_type == "message" and .chat_id == 99)' <<<"$updates" >/dev/null

conversations="$(curl -fsS -b "$cookie_jar" "$base_url/api/bots/$bot_id/conversations")"
jq -e '.conversations[0].chat_id == 99 and .conversations[0].last_message_preview == "You: operator response"' <<<"$conversations" >/dev/null

timeline="$(curl -fsS -b "$cookie_jar" "$base_url/api/bots/$bot_id/conversations/99/messages")"
jq -e '[.messages[].direction] | contains(["incoming"]) and contains(["outgoing"])' <<<"$timeline" >/dev/null
jq -e '[.messages[].text] | contains(["hello from the e2e test"]) and contains(["operator response"]) and contains(["proxied response"])' <<<"$timeline" >/dev/null

stream="$(curl -fsS -b "$cookie_jar" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d '{"name":"E2E consumer"}' \
  "$base_url/api/bots/$bot_id/stream-keys")"
stream_url="$(jq -er '.url' <<<"$stream")"
stream_data="$(curl -sS --max-time 2 "$stream_url?after=0" 2>/dev/null || true)"
grep -q 'event: update' <<<"$stream_data"
grep -q '"update_id":7001' <<<"$stream_data"

file_link="$(curl -fsS -b "$cookie_jar" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d '{"file_path":"documents/test file.txt","expires_in_seconds":300}' \
  "$base_url/api/bots/$bot_id/file-links")"
file_url="$(jq -er '.url' <<<"$file_link")"
test "$(curl -fsS "$file_url")" = "phenogram-test-file"

activity="$(curl -fsS -b "$cookie_jar" "$base_url/api/bots/$bot_id/activity")"
jq -e '[.activity[].source] | contains(["proxy"]) and contains(["bot_view"])' <<<"$activity" >/dev/null

me="$(curl -fsS -b "$cookie_jar" "$base_url/api/me")"
jq -e --arg email "$email" '.user.email == $email and .membership.retention_days == 30' <<<"$me" >/dev/null

# Bot deletion must first clean up Telegram's managed ingress webhook.
curl -fsS -X DELETE -b "$cookie_jar" \
  -H "x-phenogram-csrf: $csrf" \
  "$base_url/api/bots/$bot_id" | jq -e '.ok == true' >/dev/null
curl -fsS "$mock_url/__state" \
  | jq -e '.calls | any(.method == "deleteWebhook")' >/dev/null
curl -fsS -b "$cookie_jar" "$base_url/api/bots" \
  | jq -e --arg bot_id "$bot_id" '.bots | map(.id) | index($bot_id) | not' >/dev/null

printf 'Phenogram E2E passed: auth, bot ownership, proxy, update journal, polling, SSE, Bot View, and signed file access.\n'
