#!/usr/bin/env bash
set -euo pipefail

base_url="${PHENOGRAM_E2E_URL:-http://127.0.0.1:18080}"
mock_url="${PHENOGRAM_MOCK_URL:-http://127.0.0.1:18081}"
database_url="${PHENOGRAM_E2E_DATABASE_URL:?Set PHENOGRAM_E2E_DATABASE_URL}"

identity_subject="e2e-$(date +%s)-$$"
session_token="$(openssl rand -hex 32)"
auth_cookie="phg_session=$session_token"
bot_token="123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
child_bot_token="987654321:abcdefghijklmnopqrstuvwxyzABCDEF"
child_telegram_bot_id=987654321
child_owner_telegram_user_id=555000111
live_stream_pid=""
live_stream_dir=""

cleanup_live_stream() {
  if [[ -n "$live_stream_pid" ]]; then
    kill "$live_stream_pid" 2>/dev/null || true
    wait "$live_stream_pid" 2>/dev/null || true
    live_stream_pid=""
  fi
  if [[ -n "$live_stream_dir" && -d "$live_stream_dir" ]]; then
    rm -f -- "$live_stream_dir/events" "$live_stream_dir/headers"
    rmdir -- "$live_stream_dir"
    live_stream_dir=""
  fi
}

cleanup() {
  cleanup_live_stream
  psql "$database_url" -v ON_ERROR_STOP=1 -v subject="$identity_subject" <<'SQL' >/dev/null || true
DELETE FROM users
 WHERE id IN (
    SELECT user_id FROM oauth_identities
     WHERE provider = 'github' AND provider_subject = :'subject'
 );
SQL
}
trap cleanup EXIT

# Keep repeated local runs independent from prior mock deliveries/webhooks.
curl -fsS -X POST "$mock_url/__reset" | jq -e '.ok == true' >/dev/null
curl -fsS -X POST \
  -H 'content-type: application/json' \
  -d "{\"url\":\"$mock_url/__downstream\",\"has_custom_certificate\":false,\"allowed_updates\":[\"message\"],\"max_connections\":73}" \
  "$mock_url/__seed_webhook" | jq -e '.ok == true' >/dev/null

health="$(curl -fsS "$base_url/api/health")"
jq -e '.status == "ok" and .database == true' <<<"$health" >/dev/null

# Provider callbacks are exercised separately against the real providers. This
# service-level test seeds the same provider/user/session rows directly so it
# remains deterministic and never needs an OAuth client secret.
psql "$database_url" -v ON_ERROR_STOP=1 \
  -v subject="$identity_subject" -v session_token="$session_token" <<'SQL' >/dev/null
WITH new_user AS (
    INSERT INTO users DEFAULT VALUES RETURNING id
), new_identity AS (
    INSERT INTO oauth_identities
        (user_id, provider, provider_subject, display_name, provider_login)
    SELECT id, 'github', :'subject', 'Phenogram E2E', 'phenogram-e2e'
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
jq -e '.membership.plan_id == "free" and .membership.bot_limit == 1' <<<"$session" >/dev/null

connected="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"token\":\"$bot_token\"}" \
  "$base_url/api/bots")"
bot_id="$(jq -er '.bot.id' <<<"$connected")"
public_id="$(jq -er '.bot.public_id' <<<"$connected")"
jq -e '.bot.username == "phenogram_test_bot" and .bot.update_mode == "webhook" and (.bot | has("token_fingerprint") | not) and (.bot.public_id | length > 20) and (.warnings | any(contains("transferred")))' <<<"$connected" >/dev/null

upstream_state="$(curl -fsS "$mock_url/__state")"
ingress_secret="$(jq -er '.webhook.secret_token' <<<"$upstream_state")"
jq -e --arg public_id "$public_id" '.webhook.url | endswith("/telegram/webhook/" + $public_id)' <<<"$upstream_state" >/dev/null

# Connecting a bot automatically moves its existing Telegram webhook behind
# Phenogram before replacing Telegram's upstream destination.
migrated_webhook="$(curl -fsS "$base_url/bot$bot_token/getWebhookInfo")"
jq -e --arg url "$mock_url/__downstream" '.ok == true and .result.url == $url and .result.allowed_updates == ["message"] and .result.max_connections == 73' <<<"$migrated_webhook" >/dev/null

# Telegram sends this canonical lifecycle update to the manager. Phenogram must
# durably discover the child, fetch its credential from the manager context,
# and provision the child's own upstream webhook without user opt-in.
managed_update="$(jq -cn \
  --argjson child_bot_id "$child_telegram_bot_id" \
  --argjson owner_id "$child_owner_telegram_user_id" \
  '{
    update_id: 7100,
    managed_bot: {
      user: {id: $owner_id, is_bot: false, first_name: "Managed Owner"},
      bot: {
        id: $child_bot_id,
        is_bot: true,
        first_name: "Managed E2E Child",
        username: "managed_e2e_child_bot"
      }
    }
  }')"
curl -fsS \
  -H 'content-type: application/json' \
  -H "x-telegram-bot-api-secret-token: $ingress_secret" \
  -d "$managed_update" \
  "$base_url/telegram/webhook/$public_id" | jq -e '.ok == true' >/dev/null

bots=""
for _ in $(seq 1 100); do
  bots="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots")"
  if jq -e --argjson child_bot_id "$child_telegram_bot_id" \
    '.bots | any(.telegram_bot_id == $child_bot_id and .status == "healthy")' \
    <<<"$bots" >/dev/null; then
    break
  fi
  sleep 0.1
done

child_bot_id="$(jq -er --argjson child_bot_id "$child_telegram_bot_id" \
  '.bots[] | select(.telegram_bot_id == $child_bot_id) | .id' <<<"$bots")"
child_public_id="$(jq -er --argjson child_bot_id "$child_telegram_bot_id" \
  '.bots[] | select(.telegram_bot_id == $child_bot_id) | .public_id' <<<"$bots")"
jq -e \
  --arg manager_bot_id "$bot_id" \
  --argjson manager_telegram_bot_id 123456789 \
  --argjson child_bot_id "$child_telegram_bot_id" \
  --argjson owner_id "$child_owner_telegram_user_id" \
  '.coverage == {
      plan_id: "free",
      bot_limit: 1,
      covered_bot_count: 1,
      uncovered_bot_count: 1
    }
    and (.bots | any(
      .telegram_bot_id == $child_bot_id
      and .username == "managed_e2e_child_bot"
      and .bot_kind == "managed"
      and .is_managed == true
      and .manager_bot_id == $manager_bot_id
      and .manager_telegram_bot_id == $manager_telegram_bot_id
      and .manager_username == "phenogram_test_bot"
      and .manager_display_name == "Phenogram Test"
      and .managed_owner_telegram_user_id == $owner_id
      and .plan_covered == false
      and .effective_retention_days == 1
      and .retention_warning == "free_plan"
      and .status == "healthy"
    ))' <<<"$bots" >/dev/null

all_update_types='["message","edited_message","channel_post","edited_channel_post","business_connection","business_message","edited_business_message","deleted_business_messages","guest_message","message_reaction","message_reaction_count","inline_query","chosen_inline_result","callback_query","shipping_query","pre_checkout_query","purchased_paid_media","poll","poll_answer","my_chat_member","chat_member","chat_join_request","chat_boost","removed_chat_boost","managed_bot","subscription"]'
managed_state="$(curl -fsS "$mock_url/__state")"
child_ingress_secret="$(jq -er '.child_webhook.secret_token' <<<"$managed_state")"
jq -e \
  --arg public_id "$child_public_id" \
  '.child_webhook.url | endswith("/telegram/webhook/" + $public_id)' \
  <<<"$managed_state" >/dev/null
jq -e \
  --argjson all_update_types "$all_update_types" \
  --argjson child_bot_id "$child_telegram_bot_id" \
  '.child_webhook.allowed_updates == $all_update_types
    and .child_webhook.drop_pending_updates == false
    and (.calls | any(
      .bot == "manager"
      and (.method | ascii_downcase) == "getmanagedbottoken"
      and (.params.user_id | tonumber) == $child_bot_id
    ))
    and (.calls | any(.bot == "child" and (.method | ascii_downcase) == "getme"))
    and (.calls | any(.bot == "child" and (.method | ascii_downcase) == "getwebhookinfo"))
    and (.calls | any(.bot == "child" and (.method | ascii_downcase) == "setwebhook"))' \
  <<<"$managed_state" >/dev/null

# The mock's diagnostics are safe to inspect: neither manager nor child bot
# credentials may be copied into state or call history.
case "$managed_state" in
  *"$bot_token"*|*"$child_bot_token"*)
    printf 'Telegram mock diagnostics exposed a bot token.\n' >&2
    exit 1
    ;;
esac

credential_encrypted="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -v child_bot_id="$child_bot_id" -v child_token="$child_bot_token" <<'SQL'
SELECT token_ciphertext <> convert_to(:'child_token', 'UTF8')
       AND position(convert_to(:'child_token', 'UTF8') IN token_ciphertext) = 0
       AND octet_length(token_ciphertext) > octet_length(convert_to(:'child_token', 'UTF8'))
       AND octet_length(token_nonce) >= 24
       AND public_id <> :'child_token'
       AND token_lookup_hash <> :'child_token'
  FROM bots
 WHERE id = :'child_bot_id'::uuid;
SQL
)"
test "$credential_encrypted" = "t"

child_update='{"update_id":8100,"message":{"message_id":51,"date":1786620100,"from":{"id":707,"is_bot":false,"first_name":"Grace","username":"grace"},"chat":{"id":707,"type":"private","first_name":"Grace","username":"grace"},"text":"hello managed child"}}'
curl -fsS \
  -H 'content-type: application/json' \
  -H "x-telegram-bot-api-secret-token: $child_ingress_secret" \
  -d "$child_update" \
  "$base_url/telegram/webhook/$child_public_id" | jq -e '.ok == true' >/dev/null

child_updates="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots/$child_bot_id/updates?limit=10")"
jq -e '.updates | any(.update_id == 8100 and .event_type == "message" and .chat_id == 707)' <<<"$child_updates" >/dev/null

child_operator_reply="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d '{"chat_id":707,"text":"managed child operator response"}' \
  "$base_url/api/bots/$child_bot_id/messages")"
jq -e '.ok == true and .result.text == "managed child operator response"' <<<"$child_operator_reply" >/dev/null

child_conversations="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots/$child_bot_id/conversations")"
jq -e '.conversations[0].chat_id == 707 and .conversations[0].last_message_preview == "You: managed child operator response"' <<<"$child_conversations" >/dev/null
child_timeline="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots/$child_bot_id/conversations/707/messages")"
jq -e '[.messages[].direction] | contains(["incoming"]) and contains(["outgoing"])' <<<"$child_timeline" >/dev/null
jq -e '[.messages[].text] | contains(["hello managed child"]) and contains(["managed child operator response"])' <<<"$child_timeline" >/dev/null
curl -fsS "$mock_url/__state" \
  | jq -e '.calls | any(.bot == "child" and (.method | ascii_downcase) == "sendmessage")' >/dev/null

managed_delete="$(curl -sS -X DELETE -b "$auth_cookie" \
  -H "x-phenogram-csrf: $csrf" \
  -w $'\n%{http_code}' \
  "$base_url/api/bots/$child_bot_id")"
managed_delete_status="${managed_delete##*$'\n'}"
managed_delete_body="${managed_delete%$'\n'*}"
test "$managed_delete_status" = "409"
jq -e '.error.code == "conflict"' <<<"$managed_delete_body" >/dev/null

migrated_update='{"update_id":7000,"message":{"message_id":40,"date":1786619999,"from":{"id":99,"is_bot":false,"first_name":"Ada"},"chat":{"id":99,"type":"private","first_name":"Ada"},"text":"delivered through migrated webhook"}}'
curl -fsS \
  -H 'content-type: application/json' \
  -H "x-telegram-bot-api-secret-token: $ingress_secret" \
  -d "$migrated_update" \
  "$base_url/telegram/webhook/$public_id" | jq -e '.ok == true' >/dev/null
for _ in $(seq 1 40); do
  if curl -fsS "$mock_url/__state" | jq -e '.deliveries | any(.update_id == 7000)' >/dev/null; then
    break
  fi
  sleep 0.1
done
curl -fsS "$mock_url/__state" | jq -e '.deliveries | any(.update_id == 7000)' >/dev/null
curl -fsS \
  -H 'content-type: application/json' \
  -d '{"drop_pending_updates":false}' \
  "$base_url/bot$bot_token/deleteWebhook" | jq -e '.ok == true' >/dev/null

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

operator_reply="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d '{"chat_id":99,"text":"operator response"}' \
  "$base_url/api/bots/$bot_id/messages")"
jq -e '.ok == true and .result.text == "operator response"' <<<"$operator_reply" >/dev/null

updates="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots/$bot_id/updates?limit=10")"
jq -e '.updates | any(.update_id == 7001 and .event_type == "message" and .chat_id == 99)' <<<"$updates" >/dev/null
jq -e '.stream_cursor | type == "string" and test("^[0-9]+$")' <<<"$updates" >/dev/null
stream_cursor="$(jq -er '.stream_cursor' <<<"$updates")"

test "$(curl -sS -o /dev/null -w '%{http_code}' \
  "$base_url/api/bots/$bot_id/updates/stream?after=0")" = "401"
test "$(curl -sS -b "$auth_cookie" -o /dev/null -w '%{http_code}' \
  "$base_url/api/bots/00000000-0000-0000-0000-000000000000/updates/stream?after=0")" = "404"

console_stream="$(curl -i -sS -b "$auth_cookie" --max-time 2 \
  "$base_url/api/bots/$bot_id/updates/stream?after=0" 2>/dev/null || true)"
grep -qi '^x-accel-buffering: no' <<<"$console_stream"
grep -q 'event: update' <<<"$console_stream"
grep -q '"id":' <<<"$console_stream"
grep -q '"update_id":7001' <<<"$console_stream"
grep -q '"chat_id":99' <<<"$console_stream"
grep -q '"received_at":' <<<"$console_stream"
grep -q '"expires_at":' <<<"$console_stream"

# Native EventSource reconnects retain the original query string while adding
# Last-Event-ID. The header must win or every reconnect replays from `after=0`.
reconnected_stream="$(curl -sS -b "$auth_cookie" --max-time 1 \
  -H "Last-Event-ID: $stream_cursor" \
  "$base_url/api/bots/$bot_id/updates/stream?after=0" 2>/dev/null || true)"
if grep -q 'event: update' <<<"$reconnected_stream"; then
  echo "Last-Event-ID did not override the query cursor" >&2
  exit 1
fi

conversations="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots/$bot_id/conversations")"
jq -e '.conversations[0].chat_id == 99 and .conversations[0].last_message_preview == "You: operator response"' <<<"$conversations" >/dev/null

timeline="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots/$bot_id/conversations/99/messages")"
jq -e '[.messages[].direction] | contains(["incoming"]) and contains(["outgoing"])' <<<"$timeline" >/dev/null
jq -e '[.messages[].text] | contains(["hello from the e2e test"]) and contains(["operator response"]) and contains(["proxied response"])' <<<"$timeline" >/dev/null

# Prove that an already-connected authenticated stream receives a newly
# committed journal row, not only replayed history.
live_stream_dir="$(mktemp -d)"
curl -sS --no-buffer -b "$auth_cookie" --max-time 8 \
  -D "$live_stream_dir/headers" \
  -o "$live_stream_dir/events" \
  "$base_url/api/bots/$bot_id/updates/stream?after=$stream_cursor" &
live_stream_pid="$!"
for _ in $(seq 1 40); do
  if grep -qi '^content-type: text/event-stream' "$live_stream_dir/headers" 2>/dev/null; then
    break
  fi
  kill -0 "$live_stream_pid" 2>/dev/null
  sleep 0.05
done
grep -qi '^content-type: text/event-stream' "$live_stream_dir/headers"

live_update='{"update_id":7004,"message":{"message_id":44,"date":1786620003,"from":{"id":99,"is_bot":false,"first_name":"Ada"},"chat":{"id":99,"type":"private","first_name":"Ada"},"text":"live stream push"}}'
curl -fsS \
  -H 'content-type: application/json' \
  -H "x-telegram-bot-api-secret-token: $ingress_secret" \
  -d "$live_update" \
  "$base_url/telegram/webhook/$public_id" | jq -e '.ok == true' >/dev/null
for _ in $(seq 1 80); do
  if grep -q '"update_id":7004' "$live_stream_dir/events" 2>/dev/null; then
    break
  fi
  kill -0 "$live_stream_pid" 2>/dev/null
  sleep 0.05
done
grep -q 'event: update' "$live_stream_dir/events"
grep -Eq '^id: [0-9]+' "$live_stream_dir/events"
grep -q '"update_id":7004' "$live_stream_dir/events"
grep -q '"chat_id":99' "$live_stream_dir/events"
grep -q '"received_at":' "$live_stream_dir/events"
grep -q '"expires_at":' "$live_stream_dir/events"
cleanup_live_stream

stream="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d '{"name":"E2E consumer"}' \
  "$base_url/api/bots/$bot_id/stream-keys")"
stream_url="$(jq -er '.url' <<<"$stream")"
stream_data="$(curl -sS --max-time 2 "$stream_url?after=0" 2>/dev/null || true)"
grep -q 'event: update' <<<"$stream_data"
grep -q '"update_id":7001' <<<"$stream_data"

file_link="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d '{"file_path":"documents/test file.txt","expires_in_seconds":300}' \
  "$base_url/api/bots/$bot_id/file-links")"
file_url="$(jq -er '.url' <<<"$file_link")"
test "$(curl -fsS "$file_url")" = "phenogram-test-file"

activity="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots/$bot_id/activity")"
jq -e '[.activity[].source] | contains(["proxy"]) and contains(["bot_view"])' <<<"$activity" >/dev/null

me="$(curl -fsS -b "$auth_cookie" "$base_url/api/me")"
jq -e '.user.provider == "github" and .user.provider_login == "phenogram-e2e" and (.user | has("email") | not) and .membership.retention_days == 30' <<<"$me" >/dev/null

# Bot deletion must first clean up Telegram's managed ingress webhook.
curl -fsS -X DELETE -b "$auth_cookie" \
  -H "x-phenogram-csrf: $csrf" \
  "$base_url/api/bots/$bot_id" | jq -e '.ok == true' >/dev/null
curl -fsS "$mock_url/__state" \
  | jq -e '.calls | any(.bot == "manager" and (.method | ascii_downcase) == "deletewebhook")' >/dev/null

# Removing the manager orphans the managed bot. It remains visible with the
# one-day managerless policy and can then be explicitly deleted.
orphaned_bots="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots")"
jq -e --arg bot_id "$bot_id" --arg child_bot_id "$child_bot_id" \
  '(.bots | map(.id) | index($bot_id) | not)
    and (.bots | any(
      .id == $child_bot_id
      and .manager_bot_id == null
      and .plan_covered == false
      and .effective_retention_days == 1
      and .retention_warning == "manager_missing"
    ))' <<<"$orphaned_bots" >/dev/null
curl -fsS -X DELETE -b "$auth_cookie" \
  -H "x-phenogram-csrf: $csrf" \
  "$base_url/api/bots/$child_bot_id" | jq -e '.ok == true' >/dev/null
curl -fsS "$mock_url/__state" \
  | jq -e '.calls | any(.bot == "child" and (.method | ascii_downcase) == "deletewebhook")' >/dev/null
curl -fsS -b "$auth_cookie" "$base_url/api/bots" \
  | jq -e '.bots == []' >/dev/null

printf 'Phenogram E2E passed: auth, managed hierarchy, encrypted child credentials, proxy, update journal, polling, SSE, Bot View, and signed file access.\n'
