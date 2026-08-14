#!/usr/bin/env bash
set -euo pipefail

base_url="${PHENOGRAM_E2E_URL:-http://127.0.0.1:18080}"
mock_url="${PHENOGRAM_MOCK_URL:-http://127.0.0.1:18081}"
database_url="${PHENOGRAM_E2E_DATABASE_URL:?Set PHENOGRAM_E2E_DATABASE_URL}"

identity_subject="managed-webhook-e2e-$(date +%s)-$$"
session_token="$(openssl rand -hex 32)"
auth_cookie="phg_session=$session_token"
manager_token="123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
child_telegram_bot_id=987654321
child_owner_telegram_user_id=555000111
webhook_secret="Managed_initial_secret-1"
webhook_ip="203.0.113.29"
webhook_url="$mock_url/__downstream"

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

curl -fsS -X POST "$mock_url/__reset" | jq -e '.ok == true' >/dev/null

psql "$database_url" -v ON_ERROR_STOP=1 \
  -v subject="$identity_subject" -v session_token="$session_token" <<'SQL' >/dev/null
WITH new_user AS (
    INSERT INTO users DEFAULT VALUES RETURNING id
), new_identity AS (
    INSERT INTO oauth_identities
        (user_id, provider, provider_subject, display_name, provider_login)
    SELECT id, 'github', :'subject', 'Managed Webhook E2E', 'managed-webhook-e2e'
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
  -d "{\"token\":\"$manager_token\"}" \
  "$base_url/api/bots")"
manager_bot_id="$(jq -er '.bot.id' <<<"$connected")"
jq -e '.bot.data_plane_pool == "standard" and .bot.status == "healthy"' \
  <<<"$connected" >/dev/null

curl -fsS -X POST \
  -H 'content-type: application/json' \
  -d "{\"bot\":\"child\",\"url\":\"$webhook_url\",\"secret_token\":\"$webhook_secret\",\"allowed_updates\":[\"message\"],\"max_connections\":67,\"ip_address\":\"$webhook_ip\"}" \
  "$mock_url/__seed_webhook" | jq -e '.ok == true' >/dev/null

route_generation_before="$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -c "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE")"
psql "$database_url" -v ON_ERROR_STOP=1 \
  -v manager_bot_id="$manager_bot_id" \
  -v child_bot_id="$child_telegram_bot_id" \
  -v owner_id="$child_owner_telegram_user_id" <<'SQL' >/dev/null
INSERT INTO managed_bot_sync_jobs
       (manager_bot_id, managed_telegram_bot_id, managed_owner_telegram_user_id,
        username, display_name, source_update_id, source_update_row_id)
VALUES (:'manager_bot_id'::uuid, :'child_bot_id'::bigint, :'owner_id'::bigint,
        'managed_e2e_child_bot', 'Managed E2E Child', 0, 0);
SQL

bots=""
for _ in $(seq 1 150); do
  bots="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots")"
  if jq -e --argjson child_bot_id "$child_telegram_bot_id" \
    '.bots | any(.telegram_bot_id == $child_bot_id
      and .status == "degraded"
      and .data_plane_pool == null
      and .webhook_secret_required == true)' <<<"$bots" >/dev/null; then
    break
  fi
  sleep 0.1
done
child_bot_id="$(jq -er --argjson child_bot_id "$child_telegram_bot_id" \
  '.bots[] | select(.telegram_bot_id == $child_bot_id) | .id' <<<"$bots")"
jq -e --arg child_bot_id "$child_bot_id" \
  '.bots | any(.id == $child_bot_id
    and .status == "degraded"
    and .data_plane_pool == null
    and .webhook_secret_required == true)' <<<"$bots" >/dev/null

test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL'
SELECT data_plane_target_pool || ':' || status
  FROM bots WHERE id = :'child_bot_id'::uuid;
SQL
)" = "standard:degraded"
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL'
SELECT count(*) FROM bot_data_plane_operations WHERE bot_id = :'child_bot_id'::uuid;
SQL
)" = "0"
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -c "SELECT generation FROM data_plane_route_state WHERE singleton = TRUE")" = "$route_generation_before"

blocked_state="$(curl -fsS "$mock_url/__state")"
jq -e --arg url "$webhook_url" --arg secret "$webhook_secret" --arg ip "$webhook_ip" \
  '.child_webhook.url == $url
    and .child_webhook.secret_token == $secret
    and .child_webhook.ip_address == $ip
    and .child_webhook.allowed_updates == ["message"]
    and .child_webhook.max_connections == 67
    and ([.calls[] | select(.bot == "child" and (
      (.method | ascii_downcase) == "deletewebhook"
      or (.method | ascii_downcase) == "logout"
      or (.method | ascii_downcase) == "close"
      or (.method | ascii_downcase) == "setwebhook"))] | length) == 0' \
  <<<"$blocked_state" >/dev/null

ip_required="$(curl -sS -X POST -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"existing_webhook_secret\":\"$webhook_secret\"}" \
  -w $'\n%{http_code}' \
  "$base_url/api/bots/$child_bot_id/managed-webhook-recovery")"
test "${ip_required##*$'\n'}" = "409"
jq -e --arg ip "$webhook_ip" \
  '.error.code == "webhook_ip_address_resolution_required"
    and .error.requires_webhook_ip_address_resolution == true
    and .error.reported_ip_address == $ip' \
  <<<"${ip_required%$'\n'*}" >/dev/null

still_blocked_state="$(curl -fsS "$mock_url/__state")"
jq -e --arg url "$webhook_url" --arg secret "$webhook_secret" \
  '.child_webhook.url == $url
    and .child_webhook.secret_token == $secret
    and ([.calls[] | select(.bot == "child" and (
      (.method | ascii_downcase) == "deletewebhook"
      or (.method | ascii_downcase) == "logout"
      or (.method | ascii_downcase) == "close"
      or (.method | ascii_downcase) == "setwebhook"))] | length) == 0' \
  <<<"$still_blocked_state" >/dev/null

recovered="$(curl -fsS -X POST -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"existing_webhook_secret\":\"$webhook_secret\",\"existing_webhook_ip_address\":\"$webhook_ip\"}" \
  "$base_url/api/bots/$child_bot_id/managed-webhook-recovery")"
jq -e '.bot.status == "healthy"
  and .bot.data_plane_pool == "standard"
  and .bot.webhook_secret_required == false' <<<"$recovered" >/dev/null

recovered_state="$(curl -fsS "$mock_url/__state")"
jq -e --arg url "$webhook_url" --arg secret "$webhook_secret" --arg ip "$webhook_ip" \
  '.child_webhook.url == $url
    and .child_webhook.secret_token == $secret
    and .child_webhook.ip_address == $ip
    and .child_webhook.allowed_updates == ["message"]
    and .child_webhook.max_connections == 67
    and (.calls | any(.bot == "child" and (.method | ascii_downcase) == "deletewebhook"))
    and (.calls | any(.bot == "child" and (.method | ascii_downcase) == "logout"))
    and (.calls | any(.bot == "child" and (.method | ascii_downcase) == "setwebhook"))' \
  <<<"$recovered_state" >/dev/null

test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL'
SELECT jobs.state || ':' || COALESCE(jobs.error_summary, '')
  FROM managed_bot_sync_jobs jobs
  JOIN bots child
    ON child.manager_bot_id = jobs.manager_bot_id
   AND child.telegram_bot_id = jobs.managed_telegram_bot_id
 WHERE child.id = :'child_bot_id'::uuid;
SQL
)" = "completed:"

# A token rotation withdraws admission before reading webhook state. Keep one
# already-admitted request alive beyond the control request's polling window,
# then complete a setWebhook while it is still in flight. No Telegram mutation
# may happen until the gateway and official-server proof both report drained;
# the post-drain refetch must capture the newer webhook, not the stale one.
rotation_secret="Managed_rotation_secret-2"
rotation_ip="203.0.113.39"
curl -fsS -X POST "$mock_url/__rotate_managed_token" | jq -e '.ok == true' >/dev/null
curl -fsS -X POST \
  -H 'content-type: application/json' -d '{"in_flight":1}' \
  "$mock_url/__set_gateway_in_flight" | jq -e '.ok == true' >/dev/null
psql "$database_url" -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL' >/dev/null
UPDATE managed_bot_sync_jobs jobs
   SET source_generation = nextval('managed_bot_sync_source_generation_seq'),
       state = 'pending', attempt = 0, next_attempt_at = now(), locked_at = NULL,
       error_summary = NULL, completed_at = NULL, updated_at = now()
  FROM bots child
 WHERE child.id = :'child_bot_id'::uuid
   AND jobs.manager_bot_id = child.manager_bot_id
   AND jobs.managed_telegram_bot_id = child.telegram_bot_id;
SQL

drain_started_at="$(date +%s)"
for _ in $(seq 1 150); do
  rotation_state="$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL'
SELECT COALESCE(bots.data_plane_pool, 'none') || ':' ||
       COALESCE(operations.phase, 'none')
  FROM bots
  LEFT JOIN bot_data_plane_operations operations ON operations.bot_id = bots.id
 WHERE bots.id = :'child_bot_id'::uuid;
SQL
)"
  drain_count="$(curl -fsS "$mock_url/__state" | jq '.drain_requests | length')"
  if [[ "$rotation_state" = "none:route_withdrawn" && "$drain_count" -ge 20 ]]; then
    break
  fi
  sleep 0.1
done
test "$rotation_state" = "none:route_withdrawn"
test "$drain_count" -ge 20
test "$(( $(date +%s) - drain_started_at ))" -ge 2

drain_blocked_state="$(curl -fsS "$mock_url/__state")"
jq -e --arg url "$webhook_url" --arg secret "$webhook_secret" \
  '.child_webhook.url == $url
    and .child_webhook.secret_token == $secret
    and ([.calls[] | select(.bot == "child" and (
      (.method | ascii_downcase) == "deletewebhook"
      or (.method | ascii_downcase) == "close"
      or .credential == "rotated"))] | length) == 0' \
  <<<"$drain_blocked_state" >/dev/null

# Simulate the admitted setWebhook completing after route withdrawal. The
# gateway-local counter can disappear on restart while the official Client is
# still executing the request, so the native fence proof remains mandatory.
curl -fsS -X POST \
  -H 'content-type: application/json' \
  -d "{\"bot\":\"child\",\"url\":\"$webhook_url\",\"secret_token\":\"$rotation_secret\",\"allowed_updates\":[\"callback_query\"],\"max_connections\":71,\"ip_address\":\"$rotation_ip\"}" \
  "$mock_url/__seed_webhook" | jq -e '.ok == true' >/dev/null
curl -fsS -X POST \
  -H 'content-type: application/json' \
  -d '{"fenced":false,"standard":null,"local":null}' \
  "$mock_url/__set_gateway_official_requests" | jq -e '.ok == true' >/dev/null
curl -fsS -X POST \
  -H 'content-type: application/json' -d '{"in_flight":0}' \
  "$mock_url/__set_gateway_in_flight" | jq -e '.ok == true' >/dev/null

restart_drain_count="$drain_count"
for _ in $(seq 1 150); do
  restart_drain_count_now="$(curl -fsS "$mock_url/__state" | jq '.drain_requests | length')"
  if [[ "$restart_drain_count_now" -gt "$restart_drain_count" ]]; then
    break
  fi
  sleep 0.1
done
test "$restart_drain_count_now" -gt "$restart_drain_count"
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL'
SELECT COALESCE(bots.data_plane_pool, 'none') || ':' || operations.phase
  FROM bots
  JOIN bot_data_plane_operations operations ON operations.bot_id = bots.id
 WHERE bots.id = :'child_bot_id'::uuid;
SQL
)" = "none:route_withdrawn"
restart_blocked_state="$(curl -fsS "$mock_url/__state")"
jq -e '([.calls[] | select(.bot == "child" and (
    (.method | ascii_downcase) == "deletewebhook"
    or (.method | ascii_downcase) == "close"
    or .credential == "rotated"))] | length) == 0' \
  <<<"$restart_blocked_state" >/dev/null

# A fresh official fence that still observes an active query also blocks the
# lifecycle even though the restarted gateway reports no local in-flight work.
curl -fsS -X POST \
  -H 'content-type: application/json' \
  -d '{"fenced":true,"standard":1,"local":0}' \
  "$mock_url/__set_gateway_official_requests" | jq -e '.ok == true' >/dev/null
active_drain_count="$restart_drain_count_now"
for _ in $(seq 1 200); do
  active_drain_count_now="$(curl -fsS "$mock_url/__state" | jq '.drain_requests | length')"
  if [[ "$active_drain_count_now" -gt "$active_drain_count" ]]; then
    break
  fi
  sleep 0.1
done
test "$active_drain_count_now" -gt "$active_drain_count"
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL'
SELECT COALESCE(bots.data_plane_pool, 'none') || ':' || operations.phase
  FROM bots
  JOIN bot_data_plane_operations operations ON operations.bot_id = bots.id
 WHERE bots.id = :'child_bot_id'::uuid;
SQL
)" = "none:route_withdrawn"
curl -fsS -X POST \
  -H 'content-type: application/json' \
  -d '{"fenced":true,"standard":0,"local":0}' \
  "$mock_url/__set_gateway_official_requests" | jq -e '.ok == true' >/dev/null

rotation_bots=""
for _ in $(seq 1 600); do
  rotation_bots="$(curl -fsS -b "$auth_cookie" "$base_url/api/bots")"
  if jq -e --arg child_bot_id "$child_bot_id" \
    '.bots | any(.id == $child_bot_id
      and .status == "degraded"
      and .data_plane_pool == null
      and .webhook_secret_required == true)' <<<"$rotation_bots" >/dev/null; then
    break
  fi
  sleep 0.1
done
jq -e --arg child_bot_id "$child_bot_id" \
  '.bots | any(.id == $child_bot_id
    and .status == "degraded"
    and .data_plane_pool == null
    and .webhook_secret_required == true)' <<<"$rotation_bots" >/dev/null
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 -v child_bot_id="$child_bot_id" <<'SQL'
SELECT phase FROM bot_data_plane_operations WHERE bot_id = :'child_bot_id'::uuid;
SQL
)" = "webhook_resolution_required"

rotation_ip_required="$(curl -sS -X POST -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"existing_webhook_secret\":\"$rotation_secret\"}" \
  -w $'\n%{http_code}' \
  "$base_url/api/bots/$child_bot_id/managed-webhook-recovery")"
test "${rotation_ip_required##*$'\n'}" = "409"
jq -e --arg ip "$rotation_ip" \
  '.error.code == "webhook_ip_address_resolution_required"
    and .error.reported_ip_address == $ip' \
  <<<"${rotation_ip_required%$'\n'*}" >/dev/null
test "$(psql "$database_url" -At -v ON_ERROR_STOP=1 \
  -v child_bot_id="$child_bot_id" -v secret="$rotation_secret" <<'SQL'
SELECT webhook_resolution_ciphertext IS NOT NULL
       AND webhook_resolution_nonce IS NOT NULL
       AND position(convert_to(:'secret', 'UTF8') IN webhook_resolution_ciphertext) = 0
  FROM bot_data_plane_operations
 WHERE bot_id = :'child_bot_id'::uuid;
SQL
)" = "t"

# The secret survived only in the encrypted operation, so this retry needs to
# add the IP choice without asking for or persisting plaintext secret input.
rotation_recovered="$(curl -fsS -X POST -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"existing_webhook_ip_address\":\"$rotation_ip\"}" \
  "$base_url/api/bots/$child_bot_id/managed-webhook-recovery")"
jq -e '.bot.status == "healthy"
  and .bot.data_plane_pool == "standard"
  and .bot.webhook_secret_required == false' <<<"$rotation_recovered" >/dev/null

rotation_recovered_state="$(curl -fsS "$mock_url/__state")"
jq -e --arg url "$webhook_url" --arg secret "$rotation_secret" --arg ip "$rotation_ip" \
  '.child_webhook.url == $url
    and .child_webhook.secret_token == $secret
    and .child_webhook.ip_address == $ip
    and .child_webhook.allowed_updates == ["callback_query"]
    and .child_webhook.max_connections == 71
    and (.calls | any(.bot == "child" and .credential == "current" and (.method | ascii_downcase) == "deletewebhook"))
    and (.calls | any(.bot == "child" and .credential == "current" and (.method | ascii_downcase) == "close"))
    and (.calls | any(.bot == "child" and .credential == "rotated" and (.method | ascii_downcase) == "getme"))
    and (.calls | any(.bot == "child" and .credential == "rotated" and (.method | ascii_downcase) == "setwebhook"))' \
  <<<"$rotation_recovered_state" >/dev/null

printf 'Phenogram data-plane managed webhook E2E passed: delayed discovery and drained token rotation preserve the exact native webhook.\n'
