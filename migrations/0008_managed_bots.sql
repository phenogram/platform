ALTER TABLE bots
    ADD COLUMN bot_kind TEXT NOT NULL DEFAULT 'connected'
        CHECK (bot_kind IN ('connected', 'managed')),
    ADD COLUMN manager_bot_id UUID REFERENCES bots(id) ON DELETE SET NULL,
    ADD COLUMN manager_telegram_bot_id BIGINT,
    ADD COLUMN managed_owner_telegram_user_id BIGINT,
    ADD COLUMN token_lookup_hash TEXT,
    ADD CONSTRAINT bots_managed_hierarchy_valid CHECK (
        (
            bot_kind = 'connected'
            AND manager_bot_id IS NULL
            AND manager_telegram_bot_id IS NULL
            AND managed_owner_telegram_user_id IS NULL
        )
        OR (
            bot_kind = 'managed'
            AND manager_telegram_bot_id IS NOT NULL
            AND managed_owner_telegram_user_id IS NOT NULL
            AND manager_bot_id IS DISTINCT FROM id
        )
    );

-- public_id historically doubled as the token lookup HMAC. Split those roles
-- so managed token rotation invalidates the old credential without breaking
-- Telegram's stable webhook destination or public event/file URLs.
UPDATE bots SET token_lookup_hash = public_id;
ALTER TABLE bots ALTER COLUMN token_lookup_hash SET NOT NULL;
ALTER TABLE bots ADD CONSTRAINT bots_token_lookup_hash_unique UNIQUE (token_lookup_hash);

CREATE INDEX bots_manager_bot_id_idx ON bots (manager_bot_id, created_at, id);
CREATE INDEX bots_manager_telegram_bot_id_idx
    ON bots (user_id, manager_telegram_bot_id)
    WHERE bot_kind = 'managed';

-- The job contains identity metadata only. Managed bot tokens are fetched by a
-- worker and move directly from the Telegram response into encrypted storage.
CREATE TABLE managed_bot_sync_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    manager_bot_id UUID NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    managed_telegram_bot_id BIGINT NOT NULL,
    managed_owner_telegram_user_id BIGINT NOT NULL,
    username TEXT NOT NULL,
    display_name TEXT NOT NULL,
    source_update_id BIGINT NOT NULL,
    source_update_row_id BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'processing', 'retry', 'completed', 'conflict')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at TIMESTAMPTZ,
    error_summary TEXT,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (manager_bot_id, managed_telegram_bot_id)
);
CREATE INDEX managed_bot_sync_jobs_ready_idx
    ON managed_bot_sync_jobs (next_attempt_at, id)
    WHERE state IN ('pending', 'retry');

-- Only directly connected bots consume the hard connection limit. Managed bots
-- are discovered from authenticated manager updates and must never be dropped
-- merely because the account has exhausted its covered capacity.
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
       AND (
           memberships.status IN ('active', 'trialing')
           OR (
               memberships.status IN ('past_due', 'canceled')
               AND memberships.current_period_ends_at > now()
           )
       );
    IF allowed_bots IS NULL THEN
        RAISE EXCEPTION 'active membership required' USING ERRCODE = 'P0001';
    END IF;
    IF NEW.bot_kind = 'managed' THEN
        RETURN NEW;
    END IF;
    SELECT count(*) INTO current_bots
      FROM bots
     WHERE user_id = NEW.user_id
       AND bot_kind = 'connected';
    IF current_bots >= allowed_bots THEN
        RAISE EXCEPTION 'bot plan limit reached' USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bot_plan_covered(target_bot_id UUID) RETURNS BOOLEAN AS $$
    WITH selected AS (
        SELECT bots.id, bots.user_id, bots.bot_kind, bots.manager_bot_id,
               bots.created_at, memberships.plan_id, plans.bot_limit,
               (
                   memberships.status IN ('active', 'trialing')
                   OR (
                       memberships.status IN ('past_due', 'canceled')
                       AND memberships.current_period_ends_at > now()
                   )
               ) AS entitlements_active
          FROM bots
          JOIN memberships ON memberships.user_id = bots.user_id
          JOIN plan_definitions plans ON plans.id = memberships.plan_id
         WHERE bots.id = target_bot_id
    )
    SELECT COALESCE((
        SELECT CASE
            WHEN NOT selected.entitlements_active THEN FALSE
            WHEN selected.bot_kind = 'connected' THEN TRUE
            WHEN selected.manager_bot_id IS NULL THEN FALSE
            WHEN selected.plan_id = 'free' THEN FALSE
            ELSE (
                (
                    SELECT count(*)
                      FROM bots directly_connected
                     WHERE directly_connected.user_id = selected.user_id
                       AND directly_connected.bot_kind = 'connected'
                ) + (
                    SELECT count(*)
                      FROM bots managed
                     WHERE managed.user_id = selected.user_id
                       AND managed.bot_kind = 'managed'
                       AND managed.manager_bot_id IS NOT NULL
                       AND (managed.created_at, managed.id)
                           <= (selected.created_at, selected.id)
                )
            ) <= selected.bot_limit
        END
          FROM selected
    ), FALSE);
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION bot_effective_retention_days(target_bot_id UUID) RETURNS INTEGER AS $$
    SELECT COALESCE((
        SELECT CASE
            WHEN bots.bot_kind = 'managed' AND NOT bot_plan_covered(bots.id) THEN 1
            ELSE plans.retention_days
        END
          FROM bots
          JOIN memberships ON memberships.user_id = bots.user_id
          JOIN plan_definitions plans ON plans.id = memberships.plan_id
         WHERE bots.id = target_bot_id
    ), 1);
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION bot_retention_warning(target_bot_id UUID) RETURNS TEXT AS $$
    SELECT CASE
        WHEN bots.bot_kind <> 'managed' OR bot_plan_covered(bots.id) THEN NULL
        WHEN bots.manager_bot_id IS NULL THEN 'manager_missing'
        WHEN memberships.plan_id = 'free' THEN 'free_plan'
        ELSE 'plan_limit'
    END
      FROM bots
      JOIN memberships ON memberships.user_id = bots.user_id
     WHERE bots.id = target_bot_id;
$$ LANGUAGE sql STABLE;

-- Coverage can change when a direct manager is added or removed, an orphan is
-- reattached, or the account plan changes. Recalculate existing expiration
-- times so the stored policy matches the UI immediately.
CREATE OR REPLACE FUNCTION refresh_user_bot_retention(target_user_id UUID) RETURNS VOID AS $$
BEGIN
    UPDATE updates
       SET expires_at = received_at
           + make_interval(days => bot_effective_retention_days(bot_id))
     WHERE bot_id IN (SELECT id FROM bots WHERE user_id = target_user_id);

    UPDATE conversations
       SET expires_at = last_update_at
           + make_interval(days => bot_effective_retention_days(bot_id))
     WHERE bot_id IN (SELECT id FROM bots WHERE user_id = target_user_id);

    UPDATE api_calls
       SET expires_at = created_at
           + make_interval(days => bot_effective_retention_days(bot_id))
     WHERE bot_id IN (SELECT id FROM bots WHERE user_id = target_user_id);

    UPDATE outbound_messages
       SET expires_at = created_at
           + make_interval(days => bot_effective_retention_days(bot_id))
     WHERE bot_id IN (SELECT id FROM bots WHERE user_id = target_user_id);

    UPDATE audit_log
       SET expires_at = created_at
           + make_interval(days => bot_effective_retention_days(bot_id))
     WHERE bot_id IN (SELECT id FROM bots WHERE user_id = target_user_id);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_membership_bot_retention() RETURNS trigger AS $$
BEGIN
    PERFORM refresh_user_bot_retention(COALESCE(NEW.user_id, OLD.user_id));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER memberships_refresh_bot_retention
AFTER INSERT OR UPDATE OF plan_id, status, current_period_ends_at OR DELETE ON memberships
FOR EACH ROW EXECUTE FUNCTION refresh_membership_bot_retention();

-- Transition-table triggers refresh each affected account once. Routine status
-- and token updates still fire the statement trigger, but the function exits
-- without rewriting history unless hierarchy coverage actually changed.
CREATE OR REPLACE FUNCTION refresh_bot_retention_after_hierarchy_update() RETURNS trigger AS $$
DECLARE
    target_user_id UUID;
BEGIN
    FOR target_user_id IN
        SELECT DISTINCT changed.user_id
          FROM (
              SELECT old_bots.user_id
                FROM old_bots
                JOIN new_bots USING (id)
               WHERE old_bots.bot_kind IS DISTINCT FROM new_bots.bot_kind
                  OR old_bots.manager_bot_id IS DISTINCT FROM new_bots.manager_bot_id
                  OR old_bots.user_id IS DISTINCT FROM new_bots.user_id
              UNION
              SELECT new_bots.user_id
                FROM old_bots
                JOIN new_bots USING (id)
               WHERE old_bots.bot_kind IS DISTINCT FROM new_bots.bot_kind
                  OR old_bots.manager_bot_id IS DISTINCT FROM new_bots.manager_bot_id
                  OR old_bots.user_id IS DISTINCT FROM new_bots.user_id
          ) changed
         WHERE changed.user_id IS NOT NULL
    LOOP
        PERFORM refresh_user_bot_retention(target_user_id);
    END LOOP;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bots_refresh_retention_after_hierarchy_update
AFTER UPDATE ON bots
REFERENCING OLD TABLE AS old_bots NEW TABLE AS new_bots
FOR EACH STATEMENT EXECUTE FUNCTION refresh_bot_retention_after_hierarchy_update();

CREATE OR REPLACE FUNCTION refresh_bot_retention_after_delete() RETURNS trigger AS $$
DECLARE
    target_user_id UUID;
BEGIN
    FOR target_user_id IN SELECT DISTINCT user_id FROM deleted_bots
    LOOP
        PERFORM refresh_user_bot_retention(target_user_id);
    END LOOP;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bots_refresh_retention_after_delete
AFTER DELETE ON bots
REFERENCING OLD TABLE AS deleted_bots
FOR EACH STATEMENT EXECUTE FUNCTION refresh_bot_retention_after_delete();
