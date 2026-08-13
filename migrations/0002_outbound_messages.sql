CREATE TABLE outbound_messages (
    id BIGSERIAL PRIMARY KEY,
    bot_id UUID NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    chat_id BIGINT NOT NULL,
    telegram_message_id BIGINT,
    method TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('proxy', 'bot_view', 'webhook_response')),
    text TEXT,
    status TEXT NOT NULL CHECK (status IN ('sent', 'failed')),
    response_status INTEGER,
    error_summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX outbound_messages_conversation_idx
    ON outbound_messages (bot_id, chat_id, created_at DESC);
CREATE INDEX outbound_messages_expiry_idx ON outbound_messages (expires_at);
