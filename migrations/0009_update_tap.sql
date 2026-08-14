ALTER TABLE updates
    ADD COLUMN ingestion_source TEXT NOT NULL DEFAULT 'managed_webhook'
        CHECK (ingestion_source IN ('managed_webhook', 'official_tap', 'both'));

-- PostgreSQL is the replay source of truth. NOTIFY is deliberately only a
-- wake-up hint: listeners load the committed row by id instead of trusting a
-- payload carried by the notification channel.
CREATE OR REPLACE FUNCTION notify_phenogram_update() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(
        'phenogram_updates',
        json_build_object('bot_id', NEW.bot_id, 'row_id', NEW.id)::text
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER updates_notify_after_insert
AFTER INSERT ON updates
FOR EACH ROW EXECUTE FUNCTION notify_phenogram_update();
