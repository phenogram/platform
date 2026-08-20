import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const source = fs.readFileSync(new URL("../assets/app.js", import.meta.url), "utf8");
const styles = fs.readFileSync(new URL("../assets/app.css", import.meta.url), "utf8");
const document = {
  visibilityState: "visible",
  querySelector: () => null,
};
const window = {
  __PHENOGRAM_CHAT_TEST_MODE__: true,
  location: { origin: "https://phenogram.test", hash: "" },
  setTimeout,
  clearTimeout,
  setInterval,
  clearInterval,
  crypto: globalThis.crypto,
};

vm.runInNewContext(source, {
  window,
  document,
  URL,
  Date,
  Map,
  Set,
  console,
  setTimeout,
  clearTimeout,
  setInterval,
  clearInterval,
}, { filename: "assets/app.js" });

const chat = window.__PHENOGRAM_CHAT_TEST__;
assert.ok(chat, "chat test hooks must be exposed in test mode");

// Telegram-native authorization failures belong to the bot, not the operator session.
assert.equal(chat.isPlatformUnauthorizedPayload({ ok: false, error_code: 401, description: "Unauthorized" }), false);
assert.equal(chat.isPlatformUnauthorizedPayload({ error: { code: "unauthorized", message: "Unauthorized" } }), true);
assert.equal(chat.isTelegramFailurePayload({ ok: false, error_code: 400, description: "Bad Request" }), true);
assert.equal(chat.isTelegramFailurePayload({ ok: true, result: true }), false);
assert.equal(chat.botViewDefinitivelyRejected({ status: 200, telegramRejected: true }), true, "a 2xx Telegram ok:false envelope is a definitive rejection");
assert.equal(chat.botViewDefinitivelyRejected({ deliveryUnknown: true, status: 400, telegramRejected: true }), false, "transport ambiguity must never be presented as a safe retry");

// Sticky-bottom behavior and unread accounting.
assert.equal(chat.botViewNearBottom({ scrollHeight: 1000, scrollTop: 530, clientHeight: 400 }), true);
assert.equal(chat.botViewNearBottom({ scrollHeight: 1000, scrollTop: 300, clientHeight: 400 }), false);
assert.equal(chat.botViewUnreadAfterInsert(4, 3, false), 7);
assert.equal(chat.botViewUnreadAfterInsert(4, 3, true), 0);

// Prepending history keeps the same visible pixel anchor.
assert.equal(chat.botViewPrependScrollTop(220, 1000, 1600), 820);
assert.equal(chat.botViewPrependScrollTop(0, 1000, 900), 0);
assert.doesNotMatch(styles, /\.timeline\s*\{[^}]*scroll-behavior\s*:\s*smooth/s, "initial scroll positioning must not animate away from the bottom");
assert.match(styles, /\.app-main--bot-view\s*\{[^}]*height:\s*100dvh[^}]*overflow:\s*hidden/s, "Bot View must own the available viewport instead of growing the page");
assert.match(styles, /body\s*\{[^}]*min-height:\s*100vh;[^}]*min-height:\s*100dvh;/s, "dynamic viewport height must override the mobile 100vh fallback");
assert.match(styles, /\.app-shell\s*\{[^}]*min-height:\s*100vh;[^}]*min-height:\s*100dvh;/s, "the application shell must follow the visible mobile viewport");
assert.match(styles, /\.page\.page--bot-view\s*\{[^}]*display:\s*flex[^}]*flex:\s*1/s, "the route page must give remaining height to the chat shell");
assert.match(styles, /\.page--bot-view\s*>\s*\.bot-view\s*\{[^}]*height:\s*auto[^}]*min-height:\s*0[^}]*flex:\s*1/s, "the timeline shell must shrink internally so the composer remains visible");

// Draft text and browser File-like references survive a failed-send restore.
chat.state.selectedBotId = "bot-a";
chat.state.selectedConversationId = "conversation-a";
const draft = chat.botViewDraft();
const localFile = { name: "photo.jpg", size: 1234, url: "blob:https://phenogram.test/local" };
draft.text = "caption";
draft.files.push(localFile);
draft.deliveryUnknown = true;
assert.equal(chat.botViewDraft().text, "caption");
assert.equal(chat.botViewDraft().files[0], localFile);
assert.equal(chat.botViewDraft().deliveryUnknown, true);
chat.state.selectedConversationId = "conversation-b";
assert.equal(chat.botViewDraft().files.length, 0, "drafts must remain scoped by opaque conversation id");

// A send key can only have one owner; a stale completion cannot release it.
const sendKey = chat.botViewKey("bot-a", "conversation-a");
const firstSend = { key: sendKey };
const competingSend = { key: sendKey };
assert.equal(chat.reserveBotViewSend(sendKey, firstSend), true);
assert.equal(chat.reserveBotViewSend(sendKey, competingSend), false);
chat.finishBotViewSend(competingSend);
assert.equal(chat.reserveBotViewSend(sendKey, competingSend), false);
chat.finishBotViewSend(firstSend);
assert.equal(chat.reserveBotViewSend(sendKey, competingSend), true);
chat.finishBotViewSend(competingSend);

// SSE frames are ignored after any route/session/bot/conversation generation change.
Object.assign(chat.state, {
  route: { name: "bot-view", params: {} },
  user: { id: "user-a" },
  selectedBotId: "bot-a",
  selectedConversationId: "conversation-a",
  sessionVersion: 3,
  botContextVersion: 9,
  botViewMessagesStreamGeneration: 12,
});
const streamContext = { botId: "bot-a", conversationId: "conversation-a", sessionVersion: 3, contextVersion: 9, generation: 12 };
assert.equal(chat.botViewMessageStreamContextIsCurrent(streamContext), true);
chat.state.selectedConversationId = "conversation-b";
assert.equal(chat.botViewMessageStreamContextIsCurrent(streamContext), false);
chat.state.selectedConversationId = "conversation-a";
chat.state.botViewMessagesStreamGeneration += 1;
assert.equal(chat.botViewMessageStreamContextIsCurrent(streamContext), false);

// Server event identity wins over reusable ephemeral ids; cursor protects id=0 events.
assert.equal(chat.messageStableId({ id: "event-91", receiver_user_id: 7, ephemeral_message_id: 1 }), "event-91");
assert.equal(chat.messageStableId({ telegram_message_id: 0, cursor: "92" }), "cursor-92");
assert.equal(chat.messageStableId({ cursor: "93", receiver_user_id: 7, ephemeral_message_id: 1 }), "cursor-93");

const reusedEphemeral = {
  receiver_user_id: 7,
  messages: [
    { id: "old-generation", receiver_user_id: 7, ephemeral_message_id: 4, text: "old" },
    { id: "new-generation", receiver_user_id: 7, ephemeral_message_id: 4, text: "new" },
  ],
};
assert.equal(chat.ephemeralMessageIsActionable(reusedEphemeral.messages[0], reusedEphemeral), false);
assert.equal(chat.ephemeralMessageIsActionable(reusedEphemeral.messages[1], reusedEphemeral), true);
assert.deepEqual({ ...chat.botViewActionGenerationHeaders({ action_generation: "184467" }) }, { "x-phenogram-action-generation": "184467" });
assert.deepEqual({ ...chat.botViewActionGenerationHeaders({ action_generation: "bad\r\nheader" }) }, {});

// A snapshot may not erase events that SSE or an action response observed while it was loading.
const racedSnapshot = chat.mergeConversationMessageSnapshot(
  [{ id: "100", cursor: "100", payload: { message_id: 10, text: "snapshot" } }],
  [
    { id: "100", cursor: "100", _observed_cursor: "102", _locally_observed: true, payload: { message_id: 10, text: "edited live" } },
    { id: "101", cursor: "101", _locally_observed: true, payload: { message_id: 11, text: "new live" } },
  ],
  "100",
);
assert.equal(racedSnapshot.length, 2);
assert.equal(racedSnapshot.find((item) => item.id === "100").payload.text, "edited live");
assert.equal(racedSnapshot.find((item) => item.id === "101").payload.text, "new live");

const actionMerged = chat.mergeConversationTimelineItems(
  [{ id: "201", cursor: "201", payload: { message_id: 20, text: "old" } }],
  [{ id: "202", cursor: "202", payload: { message_id: 21, text: "accepted immediately" } }],
);
assert.equal(actionMerged.at(-1).payload.text, "accepted immediately");

const acceptedPreview = chat.mergeConversationTimelineItems([], [{
  event_type: "sendMessage",
  direction: "outgoing",
  telegram_message_id: 55,
  payload: { message_id: 55, date: 2_000, text: "accepted" },
  text: "accepted",
  status: "sent",
}]);
const durableSnapshot = chat.mergeConversationMessageSnapshot([
  { id: "outgoing-message-54", cursor: "499", direction: "outgoing", telegram_message_id: 54, payload: { message_id: 54, date: 1_999, text: "before" }, status: "sent" },
  { id: "outgoing-message-55", cursor: "500", direction: "outgoing", telegram_message_id: 55, payload: { message_id: 55, date: 2_000, text: "accepted" }, status: "sent" },
], acceptedPreview, "500");
assert.equal(durableSnapshot.filter((item) => item.telegram_message_id === 55).length, 1, "durable snapshot must replace its response preview");
assert.equal(durableSnapshot.at(-1).telegram_message_id, 55, "nested Telegram date keeps the accepted message at the tail");

const editBaseline = [{ id: "message-56", cursor: "500", direction: "outgoing", telegram_message_id: 56, payload: { message_id: 56, date: 2_001, text: "old" }, status: "sent" }];
const pendingEdit = chat.mergeConversationTimelineItems(editBaseline, [
  { event_type: "editMessageText", direction: "outgoing", telegram_message_id: 56, payload: { message_id: 56, date: 2_001, text: "accepted edit" }, status: "sent" },
]);
assert.equal(pendingEdit[0]._response_baseline_cursor, "500");
const staleAfterEdit = chat.mergeConversationMessageSnapshot(editBaseline, pendingEdit, "500");
assert.equal(staleAfterEdit[0].payload.text, "accepted edit", "a stale snapshot at the action baseline must not roll back an accepted edit");
const canonicalAfterEdit = chat.mergeConversationMessageSnapshot([
  { id: "message-56-edit", cursor: "501", direction: "outgoing", telegram_message_id: 56, actionable: true, payload: { message_id: 56, date: 2_001, edit_date: 2_002, text: "accepted edit" }, status: "sent" },
], pendingEdit, "501");
assert.equal(canonicalAfterEdit.length, 1);
assert.equal(canonicalAfterEdit[0].id, "message-56-edit");
assert.equal(canonicalAfterEdit[0]._response_pending, undefined, "the newer durable row clears the pending overlay");

const stoppedPoll = chat.mergeConversationTimelineItems([
  { id: "poll-message", cursor: "600", direction: "outgoing", telegram_message_id: 60, payload: { message_id: 60, date: 2_100, poll: { id: "poll-1", question: "Ship?", is_closed: false } } },
], [{ event_type: "stopPoll", direction: "action", telegram_message_id: 60, payload: { action: "stopPoll", request: { message_id: 60 }, telegram_result: { id: "poll-1", question: "Ship?", is_closed: true } } }]);
assert.equal(stoppedPoll.length, 1);
assert.equal(stoppedPoll[0].payload.poll.is_closed, true);

const editedEphemeral = chat.mergeConversationTimelineItems([
  { id: "eph-generation", cursor: "700", direction: "outgoing", receiver_user_id: 70, ephemeral_message_id: 7, payload: { message_id: 0, date: 2_200, text: "old" }, text: "old" },
], [{ event_type: "editEphemeralMessageText", direction: "action", receiver_user_id: 70, ephemeral_message_id: 7, payload: { action: "editEphemeralMessageText", request: { ephemeral_message_id: 7, text: "new" }, telegram_result: true } }]);
assert.equal(editedEphemeral.length, 1);
assert.equal(editedEphemeral[0].payload.text, "new");

const deletedImmediately = chat.mergeConversationTimelineItems([
  { id: "delete-target", cursor: "800", direction: "incoming", telegram_message_id: 80, payload: { message_id: 80, date: 2_300, text: "remove" }, status: "received" },
], [{ event_type: "deleteMessage", direction: "action", telegram_message_id: 80, payload: { action: "deleteMessage", request: { message_id: 80 }, telegram_result: true } }]);
assert.equal(deletedImmediately.length, 1);
assert.equal(deletedImmediately[0].status, "deleted");

const localUploadPreview = chat.botViewTimelineMessagesFromResponse({ _phenogram: { timeline_messages: [{
  event_type: "sendPhoto",
  direction: "outgoing",
  telegram_message_id: 81,
  payload: { message_id: 81, date: 2_400, photo: [{ file_id: "photo-81" }] },
  content: { media: [{ kind: "photo", url: "/api/bots/bot-a/media/photo-81" }] },
}] } }, [{ url: "blob:https://phenogram.test/upload-preview" }]);
assert.equal(localUploadPreview[0].content.media[0].url, "blob:https://phenogram.test/upload-preview");
assert.equal([...localUploadPreview[0]._local_preview_urls].join("|"), "blob:https://phenogram.test/upload-preview");

const albumUploadPreview = chat.botViewTimelineMessagesFromResponse({ _phenogram: { timeline_messages: [
  { event_type: "sendMediaGroup", direction: "outgoing", telegram_message_id: 82, payload: { message_id: 82, date: 2_401, photo: [{ file_id: "photo-82" }] }, content: { media: [{ kind: "photo", url: "/api/bots/bot-a/media/photo-82" }] } },
  { event_type: "sendMediaGroup", direction: "outgoing", telegram_message_id: 83, payload: { message_id: 83, date: 2_402, photo: [{ file_id: "photo-83" }] }, content: { media: [{ kind: "photo", url: "/api/bots/bot-a/media/photo-83" }] } },
] } }, [
  { url: "blob:https://phenogram.test/album-first" },
  { url: "blob:https://phenogram.test/album-second" },
]);
assert.equal(albumUploadPreview[0].content.media[0].url, "blob:https://phenogram.test/album-first");
assert.equal(albumUploadPreview[1].content.media[0].url, "blob:https://phenogram.test/album-second");
assert.equal([...albumUploadPreview[0]._local_preview_urls].join("|"), "blob:https://phenogram.test/album-first");
assert.equal([...albumUploadPreview[1]._local_preview_urls].join("|"), "blob:https://phenogram.test/album-second");

const canonicalUpload = chat.mergeConversationMessageSnapshot([
  { id: "durable-photo-81", cursor: "901", direction: "outgoing", telegram_message_id: 81, actionable: false, payload: { message_id: 81, date: 2_400, photo: [{ file_id: "photo-81" }] }, content: { media: [{ kind: "photo", url: "/api/bots/bot-a/media/photo-81" }] } },
], localUploadPreview, "901");
assert.equal(canonicalUpload.length, 1, "a canonical snapshot replaces the semantically identical response preview");
assert.equal(canonicalUpload[0].content.media[0].url, "/api/bots/bot-a/media/photo-81");
assert.equal(canonicalUpload[0]._local_preview_urls, undefined, "the owned blob is released only after the durable row arrives");

const durableAlbumFirst = chat.mergeConversationTimelineItems(albumUploadPreview, [
  { id: "durable-photo-82", cursor: "902", direction: "outgoing", telegram_message_id: 82, actionable: false, payload: { message_id: 82, date: 2_401, photo: [{ file_id: "photo-82" }] }, content: { media: [{ kind: "photo", url: "/api/bots/bot-a/media/photo-82" }] } },
]);
const durableFirstRow = durableAlbumFirst.find((item) => item.telegram_message_id === 82);
const pendingSecondRow = durableAlbumFirst.find((item) => item.telegram_message_id === 83);
assert.equal(durableFirstRow.content.media[0].url, "/api/bots/bot-a/media/photo-82");
assert.equal(durableFirstRow._local_preview_urls, undefined);
assert.equal(pendingSecondRow.content.media[0].url, "blob:https://phenogram.test/album-second", "one durable album row must not revoke another row's preview");

assert.equal(chat.botViewAggregateUploadLimit({ pool: "local" }), 20_000_000_000);
assert.equal(chat.botViewAggregateUploadLimit({ pool: "standard" }), 500_000_000);

// Callback updates attach once to their referenced message and never mutate the stored source row.
const callbackTarget = { id: "301", cursor: "301", payload: { message_id: 30, text: "Choose" } };
const callbackEvent = { id: "302", cursor: "302", event_type: "callback_query", payload: { callback_query: { id: "callback-1", from: { id: 9, first_name: "Ada" }, message: { message_id: 30 }, data: "choice-a" } } };
const collapsedCallbacks = chat.collapseMediaGroups([callbackTarget, callbackEvent, callbackEvent]);
assert.equal(collapsedCallbacks.length, 1);
assert.equal(collapsedCallbacks[0]._callback_events.length, 1);
assert.equal(callbackTarget._callback_events, undefined);

const normalizedCallback = chat.collapseMediaGroups([
  callbackTarget,
  { id: "303", cursor: "303", event_type: "callback_query", actionable: true, action_generation: "303", content: { kind: "callback_query", data: "choice-b", actor: { first_name: "Grace" }, target_message_id: 30 } },
]);
assert.equal(normalizedCallback.length, 1);
assert.equal(normalizedCallback[0]._callback_events[0]._action_generation, "303");
assert.match(chat.renderMessage(normalizedCallback[0], 0), /open-callback-answer/);
chat.state.botViewOpenPanel = { key: chat.botViewKey(), name: "callback-answer", data: { actionGeneration: "303" } };
assert.match(chat.renderComposerPanel("callback-answer", {}), /maxlength="200"/);

// Suggested-post moderation is only advertised in the matching direct-message context.
chat.state.selectedConversationId = "direct-conversation";
chat.state.conversations = [{ id: "direct-conversation", chat_id: -100123, direct_messages_topic_id: 77, messages: [] }];
const suggestedPostHtml = chat.renderMessage({ id: "401", direction: "incoming", payload: { message_id: 40, text: "Suggested copy", suggested_post_info: { state: "pending" } } }, 0);
assert.match(suggestedPostHtml, /data-decision="approve"/);
assert.match(suggestedPostHtml, /data-decision="decline"/);
chat.state.botViewOpenPanel = { key: chat.botViewKey(), name: "suggested-post-decline", data: { messageId: "40" } };
assert.match(chat.renderComposerPanel("suggested-post-decline", chat.state.conversations[0]), /maxlength="128"/);

// Media embeds are limited to blob previews and authenticated same-origin URLs.
assert.equal(chat.safeMediaUrl("/api/bots/bot-a/media/file-a"), "https://phenogram.test/api/bots/bot-a/media/file-a");
assert.equal(chat.safeMediaUrl("blob:https://phenogram.test/local"), "blob:https://phenogram.test/local");
assert.equal(chat.safeMediaUrl("blob:https://tracker.example/pixel"), "");
assert.equal(chat.safeMediaUrl("https://tracker.example/pixel.png"), "");
assert.equal(chat.safeMediaUrl("data:image/png;base64,AA=="), "");
assert.equal(chat.safeMediaUrl("file:///tmp/secret"), "");

console.log("chat frontend regression checks passed");
