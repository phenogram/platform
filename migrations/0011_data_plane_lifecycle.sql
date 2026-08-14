-- Telegram assigns bot identities independently in the production and test
-- environments. The numeric ID alone is therefore not globally unique.
ALTER TABLE bots
    DROP CONSTRAINT bots_telegram_bot_id_key,
    ADD COLUMN telegram_test_dc BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT bots_telegram_identity_unique
        UNIQUE (telegram_bot_id, telegram_test_dc);

-- A fingerprint is display-only and deliberately truncated; it must never be
-- an identity constraint. Fresh schemas do not have this legacy constraint,
-- but remove it defensively before environment-domain-separated values land.
ALTER TABLE bots
    DROP CONSTRAINT IF EXISTS bots_token_fingerprint_key;

CREATE INDEX bots_manager_telegram_identity_idx
    ON bots (user_id, manager_telegram_bot_id, telegram_test_dc)
    WHERE bot_kind = 'managed';

-- The collector needs to accept the target pool's tap after target login but
-- before the public route is published. This intent is durable and is never
-- included in the gateway route snapshot.
ALTER TABLE bots
    ADD COLUMN data_plane_target_pool TEXT
        CHECK (data_plane_target_pool IN ('standard', 'local'));

-- External Telegram logOut/login operations cannot be made transactional with
-- PostgreSQL. Keep a durable, secret-safe checkpoint for every in-flight bot
-- move so a crashed request can be resumed by the lifecycle worker.
CREATE TABLE bot_data_plane_operations (
    bot_id UUID PRIMARY KEY REFERENCES bots(id) ON DELETE CASCADE,
    operation TEXT NOT NULL
        CHECK (operation IN ('connect', 'managed_sync', 'managed_rotate')),
    source_pool TEXT NOT NULL
        CHECK (source_pool IN ('cloud', 'standard', 'local')),
    target_pool TEXT NOT NULL
        CHECK (target_pool IN ('standard', 'local')),
    phase TEXT NOT NULL
        CHECK (phase IN (
            'route_withdrawn',
            'webhook_resolution_required',
            'webhook_captured',
            'webhook_deleted',
            'logout_started',
            'close_started',
            'source_logged_out',
            'source_closed',
            'target_initialized',
            'webhook_restored',
            'route_published',
            'rollback_published',
            'manual_recovery'
        )),
    withdraw_generation BIGINT NOT NULL DEFAULT 0 CHECK (withdraw_generation >= 0),
    publication_generation BIGINT NOT NULL DEFAULT 0 CHECK (publication_generation >= 0),
    previous_webhook_ciphertext BYTEA,
    previous_webhook_nonce BYTEA,
    webhook_resolution_ciphertext BYTEA,
    webhook_resolution_nonce BYTEA,
    source_token_ciphertext BYTEA,
    source_token_nonce BYTEA,
    target_token_ciphertext BYTEA,
    target_token_nonce BYTEA,
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT bot_data_plane_webhook_snapshot_complete CHECK (
        (previous_webhook_ciphertext IS NULL AND previous_webhook_nonce IS NULL)
        OR (previous_webhook_ciphertext IS NOT NULL AND previous_webhook_nonce IS NOT NULL)
    ),
    CONSTRAINT bot_data_plane_webhook_resolution_complete CHECK (
        (webhook_resolution_ciphertext IS NULL AND webhook_resolution_nonce IS NULL)
        OR (webhook_resolution_ciphertext IS NOT NULL AND webhook_resolution_nonce IS NOT NULL)
    ),
    CONSTRAINT bot_data_plane_rotation_tokens_complete CHECK (
        (
            source_token_ciphertext IS NULL AND source_token_nonce IS NULL
            AND target_token_ciphertext IS NULL AND target_token_nonce IS NULL
            AND operation <> 'managed_rotate'
        )
        OR (
            source_token_ciphertext IS NOT NULL AND source_token_nonce IS NOT NULL
            AND target_token_ciphertext IS NOT NULL AND target_token_nonce IS NOT NULL
            AND operation = 'managed_rotate'
        )
    )
);

CREATE INDEX bot_data_plane_operations_ready_idx
    ON bot_data_plane_operations (next_attempt_at, updated_at, bot_id);

-- The official server replays managed lifecycle observer records until it
-- receives an acknowledgement. Persist the delivery nonce in the same
-- transaction as the managed job mutation so a lost frame/ACK or collector
-- restart is idempotent without weakening the native Bot API path.
CREATE TABLE managed_bot_lifecycle_receipts (
    data_plane_pool TEXT NOT NULL
        CHECK (data_plane_pool IN ('standard', 'local')),
    telegram_test_dc BOOLEAN NOT NULL,
    manager_bot_id UUID NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    parent_telegram_bot_id BIGINT NOT NULL CHECK (parent_telegram_bot_id > 0),
    delivery_nonce BIGINT NOT NULL CHECK (delivery_nonce > 0),
    observer_event_id INTEGER NOT NULL CHECK (observer_event_id > 0),
    managed_owner_telegram_user_id BIGINT NOT NULL
        CHECK (managed_owner_telegram_user_id > 0),
    managed_telegram_bot_id BIGINT NOT NULL CHECK (managed_telegram_bot_id > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        data_plane_pool,
        telegram_test_dc,
        manager_bot_id,
        delivery_nonce
    )
);
CREATE INDEX managed_bot_lifecycle_receipts_expiry_idx
    ON managed_bot_lifecycle_receipts (expires_at);
