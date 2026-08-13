CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE plan_definitions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    bot_limit INTEGER NOT NULL CHECK (bot_limit > 0),
    retention_days INTEGER NOT NULL CHECK (retention_days > 0),
    local_bot_api BOOLEAN NOT NULL DEFAULT FALSE,
    monthly_price_cents INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO plan_definitions (id, name, bot_limit, retention_days, local_bot_api, monthly_price_cents)
VALUES
    ('free', 'Free', 1, 30, FALSE, 0),
    ('pro', 'Pro', 5, 90, TRUE, 2900),
    ('scale', 'Scale', 25, 365, TRUE, 9900)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT users_email_normalized CHECK (email = lower(email)),
    CONSTRAINT users_email_unique UNIQUE (email)
);

CREATE TABLE memberships (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    plan_id TEXT NOT NULL REFERENCES plan_definitions(id),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'trialing', 'past_due', 'canceled')),
    current_period_ends_at TIMESTAMPTZ,
    provider_customer_id TEXT,
    provider_subscription_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash BYTEA NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    user_agent_hash BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX sessions_user_id_idx ON sessions (user_id);
CREATE INDEX sessions_expiry_idx ON sessions (expires_at);

CREATE TABLE bots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    telegram_bot_id BIGINT NOT NULL UNIQUE,
    username TEXT NOT NULL,
    display_name TEXT NOT NULL,
    token_ciphertext BYTEA NOT NULL,
    token_nonce BYTEA NOT NULL,
    token_fingerprint TEXT NOT NULL,
    public_id TEXT NOT NULL UNIQUE,
    ingress_secret_ciphertext BYTEA NOT NULL,
    ingress_secret_nonce BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'provisioning' CHECK (status IN ('provisioning', 'healthy', 'degraded', 'token_invalid', 'disabled')),
    routing_mode TEXT NOT NULL DEFAULT 'cloud' CHECK (routing_mode IN ('cloud', 'local')),
    update_mode TEXT NOT NULL DEFAULT 'polling' CHECK (update_mode IN ('polling', 'webhook')),
    webhook_migration_from_url TEXT,
    last_update_at TIMESTAMPTZ,
    last_api_call_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX bots_user_id_idx ON bots (user_id, created_at);

CREATE TABLE bot_update_state (
    bot_id UUID PRIMARY KEY REFERENCES bots(id) ON DELETE CASCADE,
    confirmed_through BIGINT,
    allowed_updates JSONB,
    downstream_webhook_url TEXT,
    downstream_secret_ciphertext BYTEA,
    downstream_secret_nonce BYTEA,
    max_connections INTEGER NOT NULL DEFAULT 40 CHECK (max_connections BETWEEN 1 AND 100),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE updates (
    id BIGSERIAL PRIMARY KEY,
    bot_id UUID NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    update_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    chat_id BIGINT,
    telegram_user_id BIGINT,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT updates_bot_update_unique UNIQUE (bot_id, update_id)
);
CREATE INDEX updates_bot_arrival_idx ON updates (bot_id, id DESC);
CREATE INDEX updates_bot_update_id_idx ON updates (bot_id, update_id);
CREATE INDEX updates_expiry_idx ON updates (expires_at);
CREATE INDEX updates_payload_gin_idx ON updates USING GIN (payload jsonb_path_ops);

CREATE TABLE conversations (
    bot_id UUID NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    chat_id BIGINT NOT NULL,
    chat_type TEXT,
    title TEXT,
    username TEXT,
    display_name TEXT,
    last_message_preview TEXT,
    last_update_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bot_id, chat_id)
);
CREATE INDEX conversations_recent_idx ON conversations (bot_id, last_update_at DESC);

CREATE TABLE api_calls (
    id BIGSERIAL PRIMARY KEY,
    bot_id UUID NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    method TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'proxy' CHECK (source IN ('proxy', 'bot_view', 'system', 'webhook_response')),
    http_status INTEGER,
    telegram_ok BOOLEAN,
    latency_ms INTEGER,
    error_summary TEXT,
    trace_id UUID NOT NULL DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX api_calls_bot_created_idx ON api_calls (bot_id, created_at DESC);

CREATE TABLE event_stream_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bot_id UUID NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    secret_hash BYTEA NOT NULL UNIQUE,
    last_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX event_stream_keys_bot_idx ON event_stream_keys (bot_id, created_at DESC);

CREATE TABLE webhook_deliveries (
    id BIGSERIAL PRIMARY KEY,
    bot_id UUID NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    update_row_id BIGINT NOT NULL REFERENCES updates(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'delivering', 'delivered', 'failed', 'discarded')),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    response_status INTEGER,
    error_summary TEXT,
    locked_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bot_id, update_row_id)
);
CREATE INDEX webhook_deliveries_ready_idx ON webhook_deliveries (next_attempt_at, id)
    WHERE state IN ('pending', 'failed');

CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    bot_id UUID REFERENCES bots(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX audit_log_user_created_idx ON audit_log (user_id, created_at DESC);
CREATE INDEX audit_log_bot_created_idx ON audit_log (bot_id, created_at DESC);

CREATE OR REPLACE FUNCTION enforce_bot_plan_limit() RETURNS trigger AS $$
DECLARE
    allowed_bots INTEGER;
    current_bots INTEGER;
BEGIN
    PERFORM 1 FROM memberships WHERE user_id = NEW.user_id FOR UPDATE;
    SELECT plans.bot_limit INTO allowed_bots
      FROM memberships memberships
      JOIN plan_definitions plans ON plans.id = memberships.plan_id
     WHERE memberships.user_id = NEW.user_id
       AND memberships.status IN ('active', 'trialing', 'past_due');
    IF allowed_bots IS NULL THEN
        RAISE EXCEPTION 'active membership required' USING ERRCODE = 'P0001';
    END IF;
    SELECT count(*) INTO current_bots FROM bots WHERE user_id = NEW.user_id;
    IF current_bots >= allowed_bots THEN
        RAISE EXCEPTION 'bot plan limit reached' USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bots_enforce_plan_limit
BEFORE INSERT ON bots
FOR EACH ROW EXECUTE FUNCTION enforce_bot_plan_limit();

