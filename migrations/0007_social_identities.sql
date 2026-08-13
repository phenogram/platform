-- Social identity is the sole account credential. Provider email addresses and
-- passwords are deliberately neither requested nor retained by Phenogram.
CREATE TABLE oauth_identities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider TEXT NOT NULL CHECK (provider IN ('google', 'github')),
    provider_subject TEXT NOT NULL CHECK (
        char_length(provider_subject) BETWEEN 1 AND 255
        AND position('@' IN provider_subject) = 0
    ),
    display_name TEXT CHECK (
        display_name IS NULL OR (
            char_length(display_name) BETWEEN 1 AND 200
            AND position('@' IN display_name) = 0
        )
    ),
    provider_login TEXT CHECK (
        provider_login IS NULL OR (
            char_length(provider_login) BETWEEN 1 AND 100
            AND position('@' IN provider_login) = 0
        )
    ),
    avatar_url TEXT CHECK (
        avatar_url IS NULL OR (
            char_length(avatar_url) BETWEEN 1 AND 2048
            AND position('@' IN avatar_url) = 0
            AND position('%40' IN lower(avatar_url)) = 0
        )
    ),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT oauth_identities_provider_subject_unique UNIQUE (provider, provider_subject)
);
CREATE INDEX oauth_identities_user_id_idx ON oauth_identities (user_id);

-- A login attempt is short-lived, browser-bound, and consumed atomically.
-- The state and browser secret are one-way digests; the PKCE verifier is
-- encrypted because it must be recovered once for the provider token exchange.
CREATE TABLE oauth_login_attempts (
    state_hash BYTEA PRIMARY KEY,
    browser_secret_hash BYTEA NOT NULL,
    provider TEXT NOT NULL CHECK (provider IN ('google', 'github')),
    pkce_verifier_ciphertext BYTEA NOT NULL,
    pkce_verifier_nonce BYTEA NOT NULL CHECK (octet_length(pkce_verifier_nonce) = 24),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX oauth_login_attempts_expiry_idx ON oauth_login_attempts (expires_at);

-- Sessions are tied to the concrete social identity used to create them. There
-- is no safe automatic mapping from a legacy email account to a provider ID, so
-- pre-migration sessions are revoked while users, memberships, and bot ownership
-- remain untouched.
ALTER TABLE sessions
    ADD COLUMN identity_id UUID REFERENCES oauth_identities(id) ON DELETE CASCADE;
DELETE FROM sessions;
ALTER TABLE sessions ALTER COLUMN identity_id SET NOT NULL;
CREATE INDEX sessions_identity_id_idx ON sessions (identity_id);

ALTER TABLE users
    DROP COLUMN email,
    DROP COLUMN password_hash;
