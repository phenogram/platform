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
tap_collector_pid=""
tap_collector_dir=""
tap_ack_listener_pid=""
telemetry_outbound_seeded=false

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
  if [[ -n "$tap_ack_listener_pid" ]]; then
    kill "$tap_ack_listener_pid" 2>/dev/null || true
    wait "$tap_ack_listener_pid" 2>/dev/null || true
    tap_ack_listener_pid=""
  fi
  if [[ -n "$tap_collector_pid" ]]; then
    kill "$tap_collector_pid" 2>/dev/null || true
    wait "$tap_collector_pid" 2>/dev/null || true
    tap_collector_pid=""
  fi
  if [[ -n "$tap_collector_dir" && -d "$tap_collector_dir" ]]; then
    rm -f -- "$tap_collector_dir/tap.sock" "$tap_collector_dir/ack.sock" \
      "$tap_collector_dir/lifecycle-acks.jsonl" "$tap_collector_dir/collector.log"
    rmdir -- "$tap_collector_dir"
    tap_collector_dir=""
  fi
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
  -d "{\"url\":\"$mock_url/__downstream\",\"has_custom_certificate\":false,\"allowed_updates\":[\"message\"],\"max_connections\":73,\"ip_address\":\"203.0.113.17\"}" \
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

secret_required="$(curl -sS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"token\":\"$bot_token\"}" \
  -w $'\n%{http_code}' \
  "$base_url/api/bots")"
secret_required_status="${secret_required##*$'\n'}"
secret_required_body="${secret_required%$'\n'*}"
test "$secret_required_status" = "409"
jq -e '.error.code == "webhook_secret_required"
  and .error.requires_webhook_secret == true
  and (.error.destination_host | length > 0)' <<<"$secret_required_body" >/dev/null

ip_resolution="$(curl -sS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"token\":\"$bot_token\",\"existing_webhook_has_no_secret\":true}" \
  -w $'\n%{http_code}' \
  "$base_url/api/bots")"
ip_resolution_status="${ip_resolution##*$'\n'}"
ip_resolution_body="${ip_resolution%$'\n'*}"
if [[ "$ip_resolution_status" = "409" ]]; then
  jq -e '.error.code == "webhook_ip_address_resolution_required"
    and .error.requires_webhook_ip_address_resolution == true
    and .error.reported_ip_address == "203.0.113.17"
    and (.error.destination_host | length > 0)' <<<"$ip_resolution_body" >/dev/null
  connected="$(curl -fsS -b "$auth_cookie" \
    -H 'content-type: application/json' \
    -H "x-phenogram-csrf: $csrf" \
    -d "{\"token\":\"$bot_token\",\"existing_webhook_has_no_secret\":true,\"existing_webhook_ip_address\":\"203.0.113.17\"}" \
    "$base_url/api/bots")"
else
  test "$ip_resolution_status" = "201"
  connected="$ip_resolution_body"
fi
bot_id="$(jq -er '.bot.id' <<<"$connected")"
public_id="$(jq -er '.bot.public_id' <<<"$connected")"
jq -e '.bot.username == "phenogram_test_bot" and .bot.update_mode == "webhook" and (.bot | has("token_fingerprint") | not) and (.bot.public_id | length > 20)' <<<"$connected" >/dev/null
if jq -e '.bot.data_plane_pool != null' <<<"$connected" >/dev/null; then
  jq -e '.webhook_ip_address_preserved == true
    and (.warnings | any(contains("chose fixed-IP continuity")))' \
    <<<"$connected" >/dev/null
else
  jq -e '.webhook_ip_address_preserved == false' <<<"$connected" >/dev/null
fi

route_generation_before="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -c "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE")"
if jq -e '.bot.data_plane_pool != null' <<<"$connected" >/dev/null; then
  psql "$database_url" -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL' >/dev/null
UPDATE bots SET data_plane_pool = NULL WHERE id = :'bot_id'::uuid;
SQL
  route_generation_withdrawn="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
    -c "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE")"
  test "$route_generation_withdrawn" -gt "$route_generation_before"
else
  route_generation_withdrawn="$route_generation_before"
fi
psql "$database_url" -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL' >/dev/null
UPDATE bots SET data_plane_pool = 'standard' WHERE id = :'bot_id'::uuid;
SQL
route_generation_active="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -c "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE")"
test "$route_generation_active" -gt "$route_generation_withdrawn"
psql "$database_url" -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL' >/dev/null
UPDATE bots SET data_plane_pool = 'standard' WHERE id = :'bot_id'::uuid;
SQL
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -c "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE")" = "$route_generation_active"

if [[ -n "${PHENOGRAM_E2E_DATA_PLANE_SYNC_TOKEN:-}" ]]; then
  route_snapshot="$(curl -fsS \
    -H 'host: phenogram' \
    -H "authorization: Bearer $PHENOGRAM_E2E_DATA_PLANE_SYNC_TOKEN" \
    "$base_url/api/internal/data-plane/routes")"
  token_lookup_hash="$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT token_lookup_hash FROM bots WHERE id = :'bot_id'::uuid;
SQL
)"
  jq -e --argjson generation "$route_generation_active" --arg hash "$token_lookup_hash" \
    '.schema_version == 1 and .generation == $generation
      and .routes == [{token_lookup_hash: $hash, pool: "standard"}]' \
    <<<"$route_snapshot" >/dev/null
  case "$route_snapshot" in *"$bot_token"*) exit 1 ;; esac

  observed_at_unix_ms="$(($(date +%s) * 1000))"
  observed_at_unix_us="${observed_at_unix_ms}000"
  telemetry_response="$(curl -fsS -X POST \
    -H 'host: phenogram' \
    -H 'content-type: application/json' \
    -H "authorization: Bearer $PHENOGRAM_E2E_DATA_PLANE_SYNC_TOKEN" \
    -d "{\"schema_version\":1,\"events\":[{\"schema_version\":1,\"token_lookup_hash\":\"$token_lookup_hash\",\"pool\":\"standard\",\"method\":\"getMe\",\"upstream_status\":200,\"latency_ms\":5,\"observed_at_unix_ms\":$observed_at_unix_ms},{\"schema_version\":1,\"kind\":\"outbound_message\",\"token_lookup_hash\":\"$token_lookup_hash\",\"pool\":\"standard\",\"method\":\"sendMessage\",\"upstream_status\":200,\"observed_at_unix_us\":$observed_at_unix_us,\"message\":{\"chat_id\":99,\"telegram_message_id\":9001,\"text\":\"gateway copy of operator response\",\"chat_type\":\"private\",\"title\":null,\"username\":\"ada\",\"display_name\":\"Ada\"}},{\"schema_version\":1,\"token_lookup_hash\":\"phg_AAAAAAAAAAAAAAAAAAAAAAAA\",\"pool\":\"standard\",\"method\":\"getMe\",\"upstream_status\":200,\"latency_ms\":5,\"observed_at_unix_ms\":$observed_at_unix_ms}]}" \
    "$base_url/api/internal/data-plane/telemetry")"
  jq -e '.ok == true and .accepted == 2 and .unknown == 1' \
    <<<"$telemetry_response" >/dev/null
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM api_calls
 WHERE bot_id = :'bot_id'::uuid
   AND source = 'data_plane'
   AND data_plane_pool = 'standard'
   AND method = 'getMe';
SQL
)" = "1"
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM outbound_messages
 WHERE bot_id = :'bot_id'::uuid
   AND chat_id = 99
   AND telegram_message_id = 9001
   AND source = 'proxy'
   AND text = 'gateway copy of operator response';
SQL
)" = "1"
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM conversations
 WHERE bot_id = :'bot_id'::uuid
   AND chat_id = 99
   AND last_message_preview = 'Bot: gateway copy of operator response';
SQL
)" = "1"
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM updates WHERE bot_id = :'bot_id'::uuid;
SQL
)" = "0"
  duplicate_outbound_response="$(curl -fsS -X POST \
    -H 'host: phenogram' \
    -H 'content-type: application/json' \
    -H "authorization: Bearer $PHENOGRAM_E2E_DATA_PLANE_SYNC_TOKEN" \
    -d "{\"schema_version\":1,\"events\":[{\"schema_version\":1,\"kind\":\"outbound_message\",\"token_lookup_hash\":\"$token_lookup_hash\",\"pool\":\"standard\",\"method\":\"sendMessage\",\"upstream_status\":200,\"observed_at_unix_us\":$observed_at_unix_us,\"message\":{\"chat_id\":99,\"telegram_message_id\":9001,\"text\":\"gateway copy of operator response\",\"chat_type\":\"private\",\"title\":null,\"username\":\"ada\",\"display_name\":\"Ada\"}}]}" \
    "$base_url/api/internal/data-plane/telemetry")"
  jq -e '.ok == true and .accepted == 1 and .unknown == 0' \
    <<<"$duplicate_outbound_response" >/dev/null
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM outbound_messages
 WHERE bot_id = :'bot_id'::uuid
   AND chat_id = 99
   AND telegram_message_id = 9001;
SQL
)" = "1"
  telemetry_outbound_seeded=true
fi

tap_collector_bin="${PHENOGRAM_TAP_COLLECTOR_BIN:-target/debug/phenogram-tap-collector}"
test -x "$tap_collector_bin"
tap_collector_dir="$(mktemp -d)"
tap_socket="$tap_collector_dir/tap.sock"
tap_ack_socket="$tap_collector_dir/ack.sock"
DATABASE_URL="$database_url" \
PHENOGRAM_TAP_POOL=standard \
PHENOGRAM_TAP_SOCKET_PATH="$tap_socket" \
PHENOGRAM_TAP_ACK_SOCKET_PATH="$tap_ack_socket" \
RUST_LOG="phenogram_platform=info" \
  "$tap_collector_bin" >"$tap_collector_dir/collector.log" 2>&1 &
tap_collector_pid="$!"
for _ in $(seq 1 100); do
  if [[ -S "$tap_socket" ]]; then
    break
  fi
  kill -0 "$tap_collector_pid" 2>/dev/null
  sleep 0.05
done
test -S "$tap_socket"

# A collector is tied to the official server pool beside it. It may observe a
# bot already active in that pool or one whose login migration targets it, but
# must drop a same-numbered event emitted by the other pool.
psql "$database_url" -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL' >/dev/null
UPDATE bots
   SET data_plane_pool = NULL, data_plane_target_pool = 'standard'
 WHERE id = :'bot_id'::uuid;
SQL
printf '%s' '{"update_id":6900,"poll":{"id":"target-pool-observer-copy"}}' \
  | python3 tests/send_tap_frame.py "$tap_socket" update 123456789 6900
for _ in $(seq 1 40); do
  target_pool_update_count="$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM updates
 WHERE bot_id = :'bot_id'::uuid AND update_id = 6900;
SQL
)"
  if [[ "$target_pool_update_count" = "1" ]]; then
    break
  fi
  sleep 0.05
done
test "$target_pool_update_count" = "1"

# A tap is an observer copy from an official Bot API server. That server owns
# the developer webhook, so stale legacy delivery state must never make
# Phenogram send the same update a second time.
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*)
  FROM webhook_deliveries deliveries
  JOIN updates ON updates.id = deliveries.update_row_id
 WHERE deliveries.bot_id = :'bot_id'::uuid
   AND updates.update_id = 6900;
SQL
)" = "0"
curl -fsS \
  -H 'content-type: application/json' \
  -d '{"update_id":6900,"poll":{"id":"native-official-delivery"}}' \
  "$mock_url/__downstream" | jq -e '.ok == true' >/dev/null
test "$(curl -fsS "$mock_url/__state" \
  | jq '[.deliveries[] | select(.update_id == 6900)] | length')" = "1"

psql "$database_url" -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL' >/dev/null
UPDATE bots
   SET data_plane_pool = 'local', data_plane_target_pool = NULL
 WHERE id = :'bot_id'::uuid;
SQL
printf '%s' '{"update_id":6901,"poll":{"id":"wrong-pool-observer-copy"}}' \
  | python3 tests/send_tap_frame.py "$tap_socket" update 123456789 6901
sleep 0.2
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM updates
 WHERE bot_id = :'bot_id'::uuid AND update_id = 6901;
SQL
)" = "0"
psql "$database_url" -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL' >/dev/null
UPDATE bots
   SET data_plane_pool = NULL, data_plane_target_pool = 'standard'
 WHERE id = :'bot_id'::uuid;
SQL

upstream_state="$(curl -fsS "$mock_url/__state")"
ingress_secret="$(jq -er '.webhook.secret_token' <<<"$upstream_state")"
jq -e --arg public_id "$public_id" '.webhook.url | endswith("/telegram/webhook/" + $public_id)' <<<"$upstream_state" >/dev/null

# Connecting a bot automatically moves its existing Telegram webhook behind
# Phenogram before replacing Telegram's upstream destination.
migrated_webhook="$(curl -fsS "$base_url/bot$bot_token/getWebhookInfo")"
jq -e --arg url "$mock_url/__downstream" '.ok == true and .result.url == $url and .result.allowed_updates == ["message"] and .result.max_connections == 73' <<<"$migrated_webhook" >/dev/null
if jq -e '.bot.data_plane_pool != null' <<<"$connected" >/dev/null; then
  jq -e '.result.ip_address == "203.0.113.17"' <<<"$migrated_webhook" >/dev/null
fi

# Telegram sends this canonical lifecycle update to the manager. Phenogram must
# durably discover the child, fetch its credential from the manager context,
# and provision the child's own upstream webhook without user opt-in.
lifecycle_producer_id=7000001
lifecycle_observer_event_id=710001
lifecycle_expiry="$(($(date +%s) + 3600))"
lifecycle_delivery_nonce=7200000001
lifecycle_ack_output="$tap_collector_dir/lifecycle-acks.jsonl"
python3 tests/send_tap_frame.py "$tap_ack_socket" ack-listen 2 \
  >"$lifecycle_ack_output" &
tap_ack_listener_pid="$!"
for _ in $(seq 1 100); do
  if [[ -S "$tap_ack_socket" ]]; then
    break
  fi
  kill -0 "$tap_ack_listener_pid" 2>/dev/null
  sleep 0.01
done
test -S "$tap_ack_socket"

PHENOGRAM_TAP_PRODUCER_ID="$lifecycle_producer_id" \
PHENOGRAM_TAP_EVENT_SEQUENCE=1 \
PHENOGRAM_TAP_OBSERVER_EVENT_ID="$lifecycle_observer_event_id" \
PHENOGRAM_TAP_EVENT_EXPIRY="$lifecycle_expiry" \
PHENOGRAM_TAP_DELIVERY_NONCE="$lifecycle_delivery_nonce" \
  python3 tests/send_tap_frame.py \
  "$tap_socket" lifecycle 123456789 \
  "$child_owner_telegram_user_id" "$child_telegram_bot_id"

# The receipt and managed-job mutation commit atomically before ACK. Replaying
# the same durable observer record with a new wire sequence must produce a new
# exact ACK without changing the already committed job generation or state.
lifecycle_receipt_count=0
lifecycle_job_count=0
for _ in $(seq 1 100); do
  lifecycle_receipt_count="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
    -v bot_id="$bot_id" -v delivery_nonce="$lifecycle_delivery_nonce" <<'SQL'
SELECT count(*)
  FROM managed_bot_lifecycle_receipts
 WHERE manager_bot_id = :'bot_id'::uuid
   AND delivery_nonce = :'delivery_nonce'::bigint;
SQL
)"
  lifecycle_job_count="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
    -v bot_id="$bot_id" -v child_bot_id="$child_telegram_bot_id" <<'SQL'
SELECT count(*)
  FROM managed_bot_sync_jobs
 WHERE manager_bot_id = :'bot_id'::uuid
   AND managed_telegram_bot_id = :'child_bot_id'::bigint;
SQL
)"
  if [[ "$lifecycle_receipt_count" = "1" && "$lifecycle_job_count" = "1" ]]; then
    break
  fi
  sleep 0.05
done
test "$lifecycle_receipt_count" = "1"
test "$lifecycle_job_count" = "1"

psql "$database_url" -v ON_ERROR_STOP=1 \
  -v bot_id="$bot_id" -v child_bot_id="$child_telegram_bot_id" <<'SQL' >/dev/null
UPDATE managed_bot_sync_jobs
   SET state = 'conflict', attempt = 77, locked_at = NULL,
       error_summary = 'durable-replay-sentinel', completed_at = NULL,
       updated_at = '2000-01-01 00:00:00+00'
 WHERE manager_bot_id = :'bot_id'::uuid
   AND managed_telegram_bot_id = :'child_bot_id'::bigint;
SQL
lifecycle_job_before="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -v bot_id="$bot_id" -v child_bot_id="$child_telegram_bot_id" <<'SQL'
SELECT source_generation, state, attempt, error_summary, updated_at
  FROM managed_bot_sync_jobs
 WHERE manager_bot_id = :'bot_id'::uuid
   AND managed_telegram_bot_id = :'child_bot_id'::bigint;
SQL
)"

PHENOGRAM_TAP_PRODUCER_ID="$lifecycle_producer_id" \
PHENOGRAM_TAP_EVENT_SEQUENCE=2 \
PHENOGRAM_TAP_OBSERVER_EVENT_ID="$lifecycle_observer_event_id" \
PHENOGRAM_TAP_EVENT_EXPIRY="$lifecycle_expiry" \
PHENOGRAM_TAP_DELIVERY_NONCE="$lifecycle_delivery_nonce" \
  python3 tests/send_tap_frame.py \
  "$tap_socket" lifecycle 123456789 \
  "$child_owner_telegram_user_id" "$child_telegram_bot_id"
wait "$tap_ack_listener_pid"
tap_ack_listener_pid=""

jq -s -e \
  --argjson producer "$lifecycle_producer_id" \
  --argjson observer_event_id "$lifecycle_observer_event_id" \
  --argjson expiry "$lifecycle_expiry" \
  --argjson delivery_nonce "$lifecycle_delivery_nonce" \
  --argjson owner_id "$child_owner_telegram_user_id" \
  --argjson child_bot_id "$child_telegram_bot_id" \
  'length == 2
    and (map(.producer) == [$producer, $producer])
    and (map(.sequence) == [1, 2])
    and all(.[ ];
      .parent_bot_id == 123456789
      and .observer_event_id == $observer_event_id
      and .expiry == $expiry
      and .delivery_nonce == $delivery_nonce
      and .owner_id == $owner_id
      and .child_id == $child_bot_id)' \
  "$lifecycle_ack_output" >/dev/null
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -v bot_id="$bot_id" -v delivery_nonce="$lifecycle_delivery_nonce" <<'SQL'
SELECT count(*)
  FROM managed_bot_lifecycle_receipts
 WHERE manager_bot_id = :'bot_id'::uuid
   AND delivery_nonce = :'delivery_nonce'::bigint;
SQL
)" = "1"
lifecycle_job_after="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -v bot_id="$bot_id" -v child_bot_id="$child_telegram_bot_id" <<'SQL'
SELECT source_generation, state, attempt, error_summary, updated_at
  FROM managed_bot_sync_jobs
 WHERE manager_bot_id = :'bot_id'::uuid
   AND managed_telegram_bot_id = :'child_bot_id'::bigint;
SQL
)"
test "$lifecycle_job_after" = "$lifecycle_job_before"

# Release the sentinel so the existing end-to-end assertions can observe the
# worker provisioning the child after the idempotency check.
psql "$database_url" -v ON_ERROR_STOP=1 \
  -v bot_id="$bot_id" -v child_bot_id="$child_telegram_bot_id" <<'SQL' >/dev/null
UPDATE managed_bot_sync_jobs
   SET state = 'pending', attempt = 0, next_attempt_at = now(), locked_at = NULL,
       error_summary = NULL, completed_at = NULL, updated_at = now()
 WHERE manager_bot_id = :'bot_id'::uuid
   AND managed_telegram_bot_id = :'child_bot_id'::bigint;
SQL

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
  --argjson owner_id "$child_owner_telegram_user_id" \
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

child_data_plane_pool="$(jq -r --arg child_bot_id "$child_bot_id" \
  '.bots[] | select(.id == $child_bot_id) | (.data_plane_pool // "")' <<<"$bots")"
if [[ -n "$child_data_plane_pool" ]]; then
  # Native setWebhook remains canonical on the official pool. Rotate the
  # managed token after installing a secret-bearing webhook: the worker must
  # withdraw and drain Phenogram admission, refetch the native webhook under
  # that fence, expose recovery, and preserve the exact webhook when the
  # operator supplies its current secret and IP intent.
  rotation_webhook_secret="Managed_rotation_secret-1"
  rotation_webhook_url="$mock_url/__downstream"
  curl -fsS \
    -H 'content-type: application/json' \
    -d "{\"url\":\"$rotation_webhook_url\",\"secret_token\":\"$rotation_webhook_secret\",\"allowed_updates\":[\"message\"],\"max_connections\":61,\"ip_address\":\"203.0.113.19\"}" \
    "$base_url/bot$child_bot_token/setWebhook" | jq -e '.ok == true' >/dev/null
  blocked_route_generation="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
    -c "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE")"
  blocked_token_lookup_hash="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
    -v child_bot_id="$child_bot_id" <<'SQL'
SELECT token_lookup_hash FROM bots WHERE id = :'child_bot_id'::uuid;
SQL
)"
  curl -fsS -X POST "$mock_url/__rotate_managed_token" | jq -e '.ok == true' >/dev/null
  python3 tests/send_tap_frame.py \
    "$tap_socket" lifecycle 123456789 \
    "$child_owner_telegram_user_id" "$child_telegram_bot_id"
  blocked_bots=""
  for _ in $(seq 1 100); do
    blocked_bots="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots")"
    if jq -e --arg child_bot_id "$child_bot_id" \
      '.bots | any(.id == $child_bot_id and .status == "degraded" and .webhook_secret_required == true)' \
      <<<"$blocked_bots" >/dev/null; then
      break
    fi
    sleep 0.1
  done
  jq -e --arg child_bot_id "$child_bot_id" \
    '.bots | any(.id == $child_bot_id
      and .status == "degraded"
      and .data_plane_pool == null
      and .webhook_secret_required == true)' \
    <<<"$blocked_bots" >/dev/null
  blocked_mock_state="$(curl -fsS "$mock_url/__state")"
  blocked_child_get_me_calls="$(jq '[.calls[] | select(.bot == "child" and (.method | ascii_downcase) == "getme")] | length' <<<"$blocked_mock_state")"
  blocked_child_webhook="$(jq -c '.child_webhook' <<<"$blocked_mock_state")"
  jq -e --arg url "$rotation_webhook_url" --arg secret "$rotation_webhook_secret" \
    '.child_webhook.url == $url
      and .child_webhook.secret_token == $secret
      and .child_webhook.allowed_updates == ["message"]
      and .child_webhook.max_connections == 61
      and .child_webhook.ip_address == "203.0.113.19"
      and ([.calls[] | select(.bot == "child" and ((.method | ascii_downcase) == "deletewebhook" or (.method | ascii_downcase) == "close" or .credential == "rotated"))] | length) == 0' \
    <<<"$blocked_mock_state" >/dev/null
  blocked_provision="$(curl -sS -X POST -b "$auth_cookie" \
    -H "x-phenogram-csrf: $csrf" \
    -w $'\n%{http_code}' \
    "$base_url/api/bots/$child_bot_id/provision")"
  blocked_provision_status="${blocked_provision##*$'\n'}"
  blocked_provision_body="${blocked_provision%$'\n'*}"
  test "$blocked_provision_status" = "409"
  jq -e '.error.code == "conflict"
    and (.error.message | contains("native webhook remains active and unchanged"))' \
    <<<"$blocked_provision_body" >/dev/null
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL'
SELECT status FROM bots WHERE id = :'child_bot_id'::uuid;
SQL
)" = "degraded"
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
    -c "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE")" -gt "$blocked_route_generation"
  blocked_mock_state_after="$(curl -fsS "$mock_url/__state")"
  test "$(jq '[.calls[] | select(.bot == "child" and (.method | ascii_downcase) == "getme")] | length' <<<"$blocked_mock_state_after")" = "$blocked_child_get_me_calls"
  test "$(jq -c '.child_webhook' <<<"$blocked_mock_state_after")" = "$blocked_child_webhook"

  recovery_ip_required="$(curl -sS -X POST -b "$auth_cookie" \
    -H 'content-type: application/json' \
    -H "x-phenogram-csrf: $csrf" \
    -d "{\"existing_webhook_secret\":\"$rotation_webhook_secret\"}" \
    -w $'\n%{http_code}' \
    "$base_url/api/bots/$child_bot_id/managed-webhook-recovery")"
  test "${recovery_ip_required##*$'\n'}" = "409"
  jq -e '.error.code == "webhook_ip_address_resolution_required"
    and .error.requires_webhook_ip_address_resolution == true
    and .error.reported_ip_address == "203.0.113.19"' \
    <<<"${recovery_ip_required%$'\n'*}" >/dev/null
  recovered_rotation="$(curl -fsS -X POST -b "$auth_cookie" \
    -H 'content-type: application/json' \
    -H "x-phenogram-csrf: $csrf" \
    -d "{\"existing_webhook_secret\":\"$rotation_webhook_secret\",\"existing_webhook_ip_address\":\"203.0.113.19\"}" \
    "$base_url/api/bots/$child_bot_id/managed-webhook-recovery")"
  jq -e '.bot.status == "healthy" and .bot.webhook_secret_required == false' \
    <<<"$recovered_rotation" >/dev/null
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL'
SELECT token_lookup_hash FROM bots WHERE id = :'child_bot_id'::uuid;
SQL
)" != "$blocked_token_lookup_hash"
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL'
SELECT jobs.state || ':' || COALESCE(jobs.error_summary, '')
  FROM managed_bot_sync_jobs jobs
  JOIN bots child
    ON child.manager_bot_id = jobs.manager_bot_id
   AND child.telegram_bot_id = jobs.managed_telegram_bot_id
 WHERE child.id = :'child_bot_id'::uuid;
SQL
)" = "completed:"
  recovered_mock_state="$(curl -fsS "$mock_url/__state")"
  jq -e --arg url "$rotation_webhook_url" --arg secret "$rotation_webhook_secret" \
    '.child_webhook.url == $url
      and .child_webhook.secret_token == $secret
      and .child_webhook.allowed_updates == ["message"]
      and .child_webhook.max_connections == 61
      and .child_webhook.ip_address == "203.0.113.19"
      and (.calls | any(.bot == "child" and .credential == "current" and (.method | ascii_downcase) == "deletewebhook"))
      and (.calls | any(.bot == "child" and .credential == "current" and (.method | ascii_downcase) == "close"))
      and (.calls | any(.bot == "child" and .credential == "rotated" and (.method | ascii_downcase) == "getme"))
      and (.calls | any(.bot == "child" and .credential == "rotated" and (.method | ascii_downcase) == "setwebhook"))' \
    <<<"$recovered_mock_state" >/dev/null
fi

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
jq -e '.ok == true and (.result | any(.update_id == 7001))' <<<"$polled" >/dev/null

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
if [[ "$telemetry_outbound_seeded" = true ]]; then
  curl -fsS -X POST \
    -H 'host: phenogram' \
    -H 'content-type: application/json' \
    -H "authorization: Bearer $PHENOGRAM_E2E_DATA_PLANE_SYNC_TOKEN" \
    -d "{\"schema_version\":1,\"events\":[{\"schema_version\":1,\"kind\":\"outbound_message\",\"token_lookup_hash\":\"$token_lookup_hash\",\"pool\":\"standard\",\"method\":\"sendMessage\",\"upstream_status\":200,\"observed_at_unix_us\":$observed_at_unix_us,\"message\":{\"chat_id\":99,\"telegram_message_id\":9001,\"text\":\"late gateway duplicate\",\"chat_type\":\"private\",\"title\":null,\"username\":\"ada\",\"display_name\":\"Ada\"}}]}" \
    "$base_url/api/internal/data-plane/telemetry" \
    | jq -e '.ok == true and .accepted == 1 and .unknown == 0' >/dev/null
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM outbound_messages
 WHERE bot_id = :'bot_id'::uuid
   AND chat_id = 99
   AND telegram_message_id = 9001
   AND source = 'bot_view'
   AND user_id IS NOT NULL
   AND text = 'operator response';
SQL
)" = "1"
fi

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

if [[ "$telemetry_outbound_seeded" = true ]]; then
  updates_before_outbound_edit="$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM updates WHERE bot_id = :'bot_id'::uuid;
SQL
)"
  edit_observed_at_unix_us="$(($(date +%s) * 1000000 + 2000000))"
  curl -fsS -X POST \
    -H 'host: phenogram' \
    -H 'content-type: application/json' \
    -H "authorization: Bearer $PHENOGRAM_E2E_DATA_PLANE_SYNC_TOKEN" \
    -d "{\"schema_version\":1,\"events\":[{\"schema_version\":1,\"kind\":\"outbound_message\",\"token_lookup_hash\":\"$token_lookup_hash\",\"pool\":\"standard\",\"method\":\"editMessageText\",\"upstream_status\":200,\"observed_at_unix_us\":$edit_observed_at_unix_us,\"message\":{\"chat_id\":99,\"telegram_message_id\":9001,\"text\":\"edited external response\",\"chat_type\":\"private\",\"title\":null,\"username\":\"ada\",\"display_name\":\"Ada\"}}]}" \
    "$base_url/api/internal/data-plane/telemetry" \
    | jq -e '.ok == true and .accepted == 1 and .unknown == 0' >/dev/null
  # Replaying the older send observation after the edit must not restore stale text.
  curl -fsS -X POST \
    -H 'host: phenogram' \
    -H 'content-type: application/json' \
    -H "authorization: Bearer $PHENOGRAM_E2E_DATA_PLANE_SYNC_TOKEN" \
    -d "{\"schema_version\":1,\"events\":[{\"schema_version\":1,\"kind\":\"outbound_message\",\"token_lookup_hash\":\"$token_lookup_hash\",\"pool\":\"standard\",\"method\":\"sendMessage\",\"upstream_status\":200,\"observed_at_unix_us\":$observed_at_unix_us,\"message\":{\"chat_id\":99,\"telegram_message_id\":9001,\"text\":\"stale original response\",\"chat_type\":\"private\",\"title\":null,\"username\":\"ada\",\"display_name\":\"Ada\"}}]}" \
    "$base_url/api/internal/data-plane/telemetry" \
    | jq -e '.ok == true and .accepted == 1 and .unknown == 0' >/dev/null
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM outbound_messages
 WHERE bot_id = :'bot_id'::uuid
   AND chat_id = 99
   AND telegram_message_id = 9001
   AND source = 'bot_view'
   AND user_id IS NOT NULL
   AND method = 'editMessageText'
   AND text = 'edited external response';
SQL
)" = "1"
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM conversations
 WHERE bot_id = :'bot_id'::uuid
   AND chat_id = 99
   AND last_message_preview = 'You: edited external response';
SQL
)" = "1"
  test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*) FROM updates WHERE bot_id = :'bot_id'::uuid;
SQL
)" = "$updates_before_outbound_edit"
fi

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
printf '%s' "$live_update" \
  | python3 tests/send_tap_frame.py "$tap_socket" update 123456789 7004
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

# During shadow rollout the managed webhook and official tap may observe the
# same Update. They must converge on one journal row without a second SSE wake.
curl -fsS \
  -H 'content-type: application/json' \
  -H "x-telegram-bot-api-secret-token: $ingress_secret" \
  -d "$live_update" \
  "$base_url/telegram/webhook/$public_id" | jq -e '.ok == true' >/dev/null
shadow_state="$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v bot_id="$bot_id" <<'SQL'
SELECT count(*)::text || ':' || min(ingestion_source)
  FROM updates
 WHERE bot_id = :'bot_id'::uuid AND update_id = 7004;
SQL
)"
test "$shadow_state" = "1:both"

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
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -c "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE")" \
  -gt "$route_generation_active"
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
