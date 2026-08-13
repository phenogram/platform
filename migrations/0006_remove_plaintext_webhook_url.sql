-- Existing webhook URLs can contain secret path or query components. Phenogram
-- only needs the in-request value for explicit takeover confirmation, not an
-- at-rest copy after the managed webhook is installed.
ALTER TABLE bots DROP COLUMN IF EXISTS webhook_migration_from_url;
