#!/bin/sh
set -eu

source_dir=${1:?usage: verify-patched-source.sh SOURCE_DIR}
client_file="$source_dir/telegram-bot-api/Client.cpp"
tap_header="$source_dir/telegram-bot-api/UpdateTap.h"
tap_source="$source_dir/telegram-bot-api/UpdateTap.cpp"
manager_file="$source_dir/telegram-bot-api/ClientManager.cpp"
parameters_file="$source_dir/telegram-bot-api/ClientParameters.h"
query_header="$source_dir/telegram-bot-api/Query.h"
query_source="$source_dir/telegram-bot-api/Query.cpp"

line_in_window() {
  awk -v begin="$2" -v end="$3" -v needle="$4" '
    index($0, begin) { inside = 1 }
    inside && index($0, needle) { print NR; exit }
    inside && index($0, end) { exit }
  ' "$1"
}

update_begin="void Client::add_update_impl("
update_end="void Client::add_new_message("
success_line=$(line_in_window "$client_file" "$update_begin" "$update_end" "if (r_id.is_ok())")
push_line=$(line_in_window "$client_file" "$update_begin" "$update_end" "tqueue_->push(tqueue_id_")
long_poll_line=$(line_in_window "$client_file" "$update_begin" "$update_end" "long_poll_wakeup(false)")
webhook_line=$(line_in_window "$client_file" "$update_begin" "$update_end" \
  "send_closure(webhook_id_, &WebhookActor::update)")
tap_line=$(line_in_window "$client_file" "$update_begin" "$update_end" \
  "parameters_->update_tap_->send_update")
failure_line=$(line_in_window "$client_file" "$update_begin" "$update_end" \
  "Update failed to be added with error")

managed_begin="void Client::add_update_managed_bot("
managed_end="void Client::add_update_subscription("
managed_native_line=$(line_in_window "$client_file" "$managed_begin" "$managed_end" \
  "add_update(UpdateType::ManagedBot")
managed_tap_line=$(line_in_window "$client_file" "$managed_begin" "$managed_end" \
  "parameters_->update_tap_->queue_managed_bot_lifecycle")

for line in "$success_line" "$push_line" "$long_poll_line" "$webhook_line" "$tap_line" "$failure_line" \
  "$managed_native_line" "$managed_tap_line"; do
  test -n "$line"
done

test "$push_line" -lt "$success_line"
test "$success_line" -lt "$long_poll_line"
test "$success_line" -lt "$webhook_line"
test "$long_poll_line" -lt "$tap_line"
test "$webhook_line" -lt "$tap_line"
test "$tap_line" -lt "$failure_line"
test "$managed_native_line" -lt "$managed_tap_line"

grep -Fq "static_cast<td::uint64>(my_id_)" "$client_file"
grep -Fq "static_cast<td::uint32>(id.value())" "$client_file"
grep -Fq "if (!update_tap_socket.empty())" "$source_dir/telegram-bot-api/telegram-bot-api.cpp"
grep -Fq -- "--update-tap-socket and --update-tap-ack-socket must be specified together" \
  "$source_dir/telegram-bot-api/telegram-bot-api.cpp"
grep -Fq "MANAGED_LIFECYCLE_QUEUE_ID = -0x50475554LL" "$tap_header"
grep -Fq "MAX_MANAGED_LIFECYCLE_EVENTS = 10000" "$tap_header"
grep -Fq "MANAGED_LIFECYCLE_RETENTION_SECONDS = 7 * 86400" "$tap_header"
grep -Fq "tqueue.push(MANAGED_LIFECYCLE_QUEUE_ID" "$tap_source"
grep -Fq 'std::memcmp(ack.data(), "PGUA", 4)' "$tap_source"
grep -Fq "tqueue.forget(MANAGED_LIFECYCLE_QUEUE_ID" "$tap_source"
grep -Fq "replay_managed_bot_lifecycle" "$manager_file"
grep -Fq 'query->get_header("x-phenogram-route-generation")' "$manager_file"
grep -Fq "is_gateway_query_fenced" "$manager_file"
grep -Fq 'phenogram_action == "drain"' "$manager_file"
grep -Fq "install_gateway_drain_fence" "$manager_file" "$parameters_file"
grep -Fq "get_active_bot_query_count" "$manager_file" "$parameters_file"
grep -Fq "register_active_bot_query(token_, is_test_dc_)" "$query_source"
grep -Fq "unregister_active_bot_query(active_bot_id_, is_test_dc_)" "$query_header"

if grep -Fq "bot_token" "$tap_header" "$tap_source"; then
  echo "Update tap must not accept, store, or serialize bot tokens" >&2
  exit 1
fi

echo "Verified native-first tap ordering, bounded durable lifecycle replay/ACK, ordered gateway drain fence, opt-in behavior, and tap token exclusion"
