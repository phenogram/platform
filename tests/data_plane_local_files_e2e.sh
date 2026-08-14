#!/usr/bin/env bash
set -euo pipefail

base_url="${PHENOGRAM_E2E_URL:-http://127.0.0.1:18080}"
mock_url="${PHENOGRAM_MOCK_URL:-http://127.0.0.1:18081}"
database_url="${PHENOGRAM_E2E_DATABASE_URL:?Set PHENOGRAM_E2E_DATABASE_URL}"
identity_subject="e2e-data-plane-local-files-$(date +%s)-$$"
session_token="$(openssl rand -hex 32)"
auth_cookie="phg_session=$session_token"
prod_token="123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"
test_token="123456789:TESTABCDEFGHIJKLMNOPQRSTUVWXYZabcd"
download_dir=""

cleanup() {
  if [[ -n "$download_dir" && -d "$download_dir" ]]; then
    rm -f -- "$download_dir/headers" "$download_dir/body"
    rmdir -- "$download_dir"
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

curl -fsS -X POST "$mock_url/__reset" | jq -e '.ok == true' >/dev/null
psql "$database_url" -v ON_ERROR_STOP=1 \
  -v subject="$identity_subject" -v session_token="$session_token" <<'SQL' >/dev/null
WITH new_user AS (
    INSERT INTO users DEFAULT VALUES RETURNING id
), new_identity AS (
    INSERT INTO oauth_identities
        (user_id, provider, provider_subject, display_name, provider_login)
    SELECT id, 'github', :'subject', 'Local file E2E', 'local-file-e2e'
      FROM new_user
    RETURNING id, user_id
), new_membership AS (
    INSERT INTO memberships (user_id, plan_id, status)
    SELECT user_id, 'pro', 'active' FROM new_identity
)
INSERT INTO sessions (user_id, identity_id, token_hash, expires_at)
SELECT user_id, id, digest(:'session_token', 'sha256'), now() + interval '1 hour'
  FROM new_identity;
SQL

csrf="$(curl -fsS -b "$auth_cookie" "$base_url/api/me" | jq -er '.csrf_token')"
test_bot="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"token\":\"$test_token\",\"test_dc\":true,\"pool\":\"local\"}" \
  "$base_url/api/bots")"
test_bot_id="$(jq -er '.bot.id' <<<"$test_bot")"
jq -e '.bot.telegram_test_dc == true
  and .bot.data_plane_pool == "local"
  and .bot.status == "healthy"' <<<"$test_bot" >/dev/null

file_info="$(curl -fsS \
  -H 'content-type: application/json' \
  -d '{"file_id":"native-local-file"}' \
  "$mock_url/bot$test_token/test/getFile")"
native_path="$(jq -er '.result.file_path' <<<"$file_info")"
test "$native_path" = "/var/lib/telegram-bot-api/$test_token:T/documents/test.txt"

link="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "$(jq -cn --arg path "$native_path" '{file_path:$path, expires_in_seconds:300}')" \
  "$base_url/api/bots/$test_bot_id/file-links")"
file_url="$(jq -er '.url' <<<"$link")"
opaque_path="$(jq -er '.url | capture("/files/(?<value>[^?]+)").value' <<<"$link")"
case "$file_url" in
  *"$test_token"*|*"$native_path"*)
    printf 'Signed local file URL exposed the native token-bearing path.\n' >&2
    exit 1
    ;;
esac
case "$opaque_path" in
  __phenogram_local__/*) ;;
  *) exit 1 ;;
esac

download_dir="$(mktemp -d)"
curl -fsS \
  -H 'range: bytes=0-8' \
  -D "$download_dir/headers" \
  -o "$download_dir/body" \
  "$file_url"
grep -Eq '^HTTP/[0-9.]+ 206' "$download_dir/headers"
grep -Eqi '^content-range: bytes 0-8/20' "$download_dir/headers"
grep -Eqi '^accept-ranges: bytes' "$download_dir/headers"
test "$(<"$download_dir/body")" = "phenogram"
curl -fsS "$mock_url/__state" | jq -e \
  --arg native_path "$native_path" \
  '.file_requests | any(.bot == "manager"
    and .test_dc == true
    and .file_path == $native_path
    and .range == "bytes=0-8")' >/dev/null

opaque_prefix="${opaque_path%?}"
last_character="${opaque_path#"$opaque_prefix"}"
replacement="A"
[[ "$last_character" == "A" ]] && replacement="B"
tampered_path="$opaque_prefix$replacement"
tampered_url="${file_url/$opaque_path/$tampered_path}"
test "$(curl -sS -o /dev/null -w '%{http_code}' "$tampered_url")" = "403"

prod_bot="$(curl -fsS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "{\"token\":\"$prod_token\",\"pool\":\"local\"}" \
  "$base_url/api/bots")"
prod_bot_id="$(jq -er '.bot.id' <<<"$prod_bot")"
wrong_bot="$(curl -sS -b "$auth_cookie" \
  -H 'content-type: application/json' \
  -H "x-phenogram-csrf: $csrf" \
  -d "$(jq -cn --arg path "$opaque_path" '{file_path:$path}')" \
  -w $'\n%{http_code}' \
  "$base_url/api/bots/$prod_bot_id/file-links")"
test "${wrong_bot##*$'\n'}" = "422"
jq -e '.error.code == "invalid_request"' <<<"${wrong_bot%$'\n'*}" >/dev/null

printf 'Data-plane Local file E2E passed: opaque signed path, bot binding, tamper rejection, and byte-range forwarding.\n'
