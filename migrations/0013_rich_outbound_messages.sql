-- The Bot View timeline keeps the Telegram Message returned by successful
-- outbound calls. Telegram bot tokens and resolved file paths are never stored
-- here; file_id values are rendered through an authenticated proxy.
ALTER TABLE outbound_messages
    ADD COLUMN payload JSONB,
    ADD COLUMN dedupe_key TEXT;

-- A Telegram numeric chat id is not a conversation identity on its own.
-- Business connections, guest queries, forum topics, and direct-message
-- topics can legally reuse the same chat id while requiring different send
-- context. Give every context an opaque id so the console cannot merge them.
ALTER TABLE conversations
    DROP CONSTRAINT conversations_pkey,
    ADD COLUMN id UUID NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN business_connection_id TEXT,
    ADD COLUMN guest_query_id TEXT,
    ADD COLUMN message_thread_id BIGINT,
    ADD COLUMN direct_messages_topic_id BIGINT,
    ADD COLUMN receiver_user_id BIGINT,
    ADD CONSTRAINT conversations_pkey PRIMARY KEY (id),
    ADD CONSTRAINT conversations_identity_unique UNIQUE NULLS NOT DISTINCT
        (bot_id, chat_id, business_connection_id, guest_query_id,
         message_thread_id, direct_messages_topic_id, receiver_user_id);

ALTER TABLE updates
    ADD COLUMN conversation_id UUID REFERENCES conversations(id) ON DELETE SET NULL,
    ADD COLUMN telegram_message_id BIGINT,
    ADD COLUMN ephemeral_message_id BIGINT,
    ADD COLUMN edit_date TIMESTAMPTZ;
ALTER TABLE outbound_messages
    ADD COLUMN conversation_id UUID REFERENCES conversations(id) ON DELETE SET NULL,
    ADD COLUMN receiver_user_id BIGINT,
    ADD COLUMN ephemeral_message_id BIGINT;

UPDATE updates AS updates
   SET conversation_id = conversations.id
  FROM conversations
 WHERE conversations.bot_id = updates.bot_id
   AND conversations.chat_id = updates.chat_id
   AND conversations.business_connection_id IS NULL
   AND conversations.guest_query_id IS NULL
   AND conversations.message_thread_id IS NULL
   AND conversations.direct_messages_topic_id IS NULL;

UPDATE outbound_messages AS messages
   SET conversation_id = conversations.id
  FROM conversations
 WHERE conversations.bot_id = messages.bot_id
   AND conversations.chat_id = messages.chat_id
   AND conversations.business_connection_id IS NULL
   AND conversations.guest_query_id IS NULL
   AND conversations.message_thread_id IS NULL
   AND conversations.direct_messages_topic_id IS NULL;

UPDATE outbound_messages
   SET dedupe_key = 'message:' || bot_id::text || ':' || conversation_id::text || ':'
                    || telegram_message_id::text
 WHERE conversation_id IS NOT NULL
   AND telegram_message_id IS NOT NULL
   AND telegram_message_id <> 0;
CREATE UNIQUE INDEX outbound_messages_dedupe_key_idx
    ON outbound_messages (dedupe_key)
    WHERE dedupe_key IS NOT NULL;

CREATE INDEX updates_conversation_idx
    ON updates (conversation_id, received_at DESC)
    WHERE conversation_id IS NOT NULL;
CREATE INDEX outbound_messages_conversation_id_idx
    ON outbound_messages (conversation_id, created_at DESC)
    WHERE conversation_id IS NOT NULL;

DROP INDEX outbound_messages_telegram_identity_idx;
CREATE UNIQUE INDEX outbound_messages_telegram_identity_idx
    ON outbound_messages (bot_id, conversation_id, telegram_message_id)
    WHERE conversation_id IS NOT NULL AND telegram_message_id IS NOT NULL
      AND telegram_message_id <> 0;
CREATE TABLE conversation_events (
    id BIGSERIAL PRIMARY KEY,
    bot_id UUID NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    direction TEXT NOT NULL CHECK (direction IN ('incoming', 'outgoing', 'action')),
    event_type TEXT NOT NULL,
    source_table TEXT NOT NULL CHECK (source_table IN ('updates', 'outbound_messages', 'bot_view_action')),
    source_id BIGINT,
    telegram_message_id BIGINT,
    receiver_user_id BIGINT,
    ephemeral_message_id BIGINT,
    edit_date TIMESTAMPTZ,
    text TEXT,
    status TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX conversation_events_source_idx
    ON conversation_events (source_table, source_id)
    WHERE source_id IS NOT NULL;
CREATE INDEX conversation_events_timeline_idx
    ON conversation_events (conversation_id, id DESC);
CREATE INDEX conversation_events_expiry_idx ON conversation_events (expires_at);

INSERT INTO conversation_events
       (bot_id, conversation_id, direction, event_type, source_table, source_id,
        telegram_message_id, receiver_user_id, ephemeral_message_id, edit_date,
        text, status, payload, created_at, expires_at)
SELECT updates.bot_id, updates.conversation_id, 'incoming', updates.event_type,
       'updates', updates.id, updates.telegram_message_id, NULL,
       updates.ephemeral_message_id, updates.edit_date, NULL, 'received', updates.payload,
       updates.received_at, updates.expires_at
  FROM updates
 WHERE updates.conversation_id IS NOT NULL;

INSERT INTO conversation_events
       (bot_id, conversation_id, direction, event_type, source_table, source_id,
        telegram_message_id, receiver_user_id, ephemeral_message_id, edit_date,
        text, status, payload, created_at, expires_at)
SELECT messages.bot_id, messages.conversation_id, 'outgoing', messages.method,
       'outbound_messages', messages.id, messages.telegram_message_id,
       messages.receiver_user_id, messages.ephemeral_message_id,
       NULL, messages.text, messages.status, messages.payload, messages.created_at,
       messages.expires_at
  FROM outbound_messages AS messages
 WHERE messages.conversation_id IS NOT NULL;

CREATE INDEX outbound_messages_media_group_idx
    ON outbound_messages (bot_id, chat_id, ((payload ->> 'media_group_id')), created_at)
    WHERE payload ? 'media_group_id';
