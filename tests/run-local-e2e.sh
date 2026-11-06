#!/usr/bin/env bash
# Disposable PostgreSQL + Telegram mock + real application. No production secrets.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -n "${PG_BIN:-}" ]]; then
  export PATH="$PG_BIN:$PATH"
elif [[ -d /opt/homebrew/opt/postgresql@17/bin ]]; then
  export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"
elif command -v pg_config >/dev/null; then
  export PATH="$(pg_config --bindir):$PATH"
fi
for command in initdb pg_ctl createdb psql curl jq python3 cargo; do
  command -v "$command" >/dev/null || { printf 'Required command missing: %s\n' "$command" >&2; exit 1; }
done

work="$(mktemp -d "${TMPDIR:-/tmp}/phenogram-e2e.XXXXXX")"
app_pid=""; mock_pid=""; pg_started=false
cleanup() {
  status=$?
  trap - EXIT
  for pid in "$app_pid" "$mock_pid"; do
    if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  done
  if $pg_started; then pg_ctl -D "$work/db" -m fast -w stop >/dev/null; fi
  if [[ "$status" -ne 0 ]]; then
    printf 'E2E failed; disposable logs/database retained at %s\n' "$work" >&2
    tail -n 30 "$work/app.log" 2>/dev/null || true
  else
    # Exact directory created above, containing only disposable test artifacts.
    rm -rf -- "$work"
  fi
  exit "$status"
}
trap cleanup EXIT
app_port="${PHENOGRAM_TEST_APP_PORT:-18980}"
mock_port="${PHENOGRAM_TEST_MOCK_PORT:-18981}"
pg_port="${PHENOGRAM_TEST_PG_PORT:-18982}"
cargo build --locked --bins
initdb -D "$work/db" -U phenogram --auth=trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$work/db" -l "$work/postgres.log" -o "-h 127.0.0.1 -p $pg_port -k $work" -w start >/dev/null
pg_started=true
createdb -h 127.0.0.1 -p "$pg_port" -U phenogram phenogram_e2e
export DATABASE_URL="postgresql://phenogram@127.0.0.1:$pg_port/phenogram_e2e"
export PHENOGRAM_E2E_DATABASE_URL="$DATABASE_URL"
export PHENOGRAM_E2E_URL="http://127.0.0.1:$app_port"
export PHENOGRAM_MOCK_URL="http://127.0.0.1:$mock_port"
export APP_ENV=test LISTEN_ADDR="127.0.0.1:$app_port"
export LANDING_BASE_URL="$PHENOGRAM_E2E_URL" APP_BASE_URL="$PHENOGRAM_E2E_URL" API_BASE_URL="$PHENOGRAM_E2E_URL"
export MASTER_KEY=local-e2e-master-key-not-a-production-secret
export PUBLIC_ID_KEY=local-e2e-public-key-not-a-production-secret
export LINK_SIGNING_KEY=local-e2e-link-key-not-a-production-secret
export TELEGRAM_CLOUD_API_URL="$PHENOGRAM_MOCK_URL" TELEGRAM_LOCAL_API_URL="$PHENOGRAM_MOCK_URL"
export TELEGRAM_LOCAL_DATA_DIR="" DATA_PLANE_ENABLED=false DATA_PLANE_SYNC_TOKEN=""
export GOOGLE_OAUTH_CLIENT_ID="" GOOGLE_OAUTH_CLIENT_SECRET="" GITHUB_OAUTH_CLIENT_ID="" GITHUB_OAUTH_CLIENT_SECRET=""
unset PHENOGRAM_E2E_DATA_PLANE_SYNC_TOKEN
MOCK_TELEGRAM_PORT="$mock_port" python3 tests/mock_telegram.py >"$work/mock.log" 2>&1 &
mock_pid=$!
RUST_LOG=warn target/debug/phenogram-platform >"$work/app.log" 2>&1 &
app_pid=$!
for attempt in {1..100}; do
  if curl -fsS "$PHENOGRAM_E2E_URL/api/health" >/dev/null 2>&1; then break; fi
  kill -0 "$app_pid" 2>/dev/null || { printf 'Test application failed to start\n' >&2; exit 1; }
  sleep 0.1
done
curl -fsS "$PHENOGRAM_E2E_URL/api/health" | jq -e '.status == "ok" and .database' >/dev/null
bash tests/e2e.sh
