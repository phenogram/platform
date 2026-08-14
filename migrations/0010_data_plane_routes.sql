ALTER TABLE bots
    ADD COLUMN data_plane_pool TEXT
        CHECK (data_plane_pool IN ('standard', 'local'));

-- A row id cannot fence lifecycle-only managed-bot signals because those
-- signals intentionally do not create a synthetic Telegram Update. This
-- durable generation protects an in-flight worker from completing over a
-- newer canonical update or owner signal.
CREATE SEQUENCE managed_bot_sync_source_generation_seq;
ALTER TABLE managed_bot_sync_jobs
    ADD COLUMN source_generation BIGINT NOT NULL
        DEFAULT nextval('managed_bot_sync_source_generation_seq');
ALTER SEQUENCE managed_bot_sync_source_generation_seq
    OWNED BY managed_bot_sync_jobs.source_generation;

ALTER TABLE api_calls DROP CONSTRAINT api_calls_source_check;
ALTER TABLE api_calls
    ADD CONSTRAINT api_calls_source_check
        CHECK (source IN ('proxy', 'bot_view', 'system', 'webhook_response', 'data_plane')),
    ADD COLUMN data_plane_pool TEXT
        CHECK (data_plane_pool IN ('standard', 'local'));

CREATE TABLE data_plane_route_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    generation BIGINT NOT NULL CHECK (generation > 0)
);

INSERT INTO data_plane_route_state (singleton, generation)
VALUES (TRUE, 1)
ON CONFLICT (singleton) DO NOTHING;

CREATE OR REPLACE FUNCTION bump_data_plane_route_generation() RETURNS VOID AS $$
BEGIN
    UPDATE data_plane_route_state
       SET generation = generation + 1
     WHERE singleton = TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bump_data_plane_routes_for_bot() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.data_plane_pool IS NOT NULL THEN
            PERFORM bump_data_plane_route_generation();
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        IF OLD.data_plane_pool IS NOT NULL THEN
            PERFORM bump_data_plane_route_generation();
        END IF;
    ELSIF (OLD.data_plane_pool, OLD.token_lookup_hash)
          IS DISTINCT FROM
          (NEW.data_plane_pool, NEW.token_lookup_hash)
          AND (OLD.data_plane_pool IS NOT NULL OR NEW.data_plane_pool IS NOT NULL) THEN
        PERFORM bump_data_plane_route_generation();
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bots_bump_data_plane_routes
AFTER INSERT OR DELETE OR UPDATE OF data_plane_pool, token_lookup_hash ON bots
FOR EACH ROW EXECUTE FUNCTION bump_data_plane_routes_for_bot();
