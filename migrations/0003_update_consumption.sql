ALTER TABLE updates
    ADD COLUMN IF NOT EXISTS consumed_at TIMESTAMPTZ;

-- Preserve completed webhook delivery and polling cursor semantics when this
-- migration is applied to an existing installation.
UPDATE updates AS updates
   SET consumed_at = COALESCE(deliveries.delivered_at, now())
  FROM webhook_deliveries AS deliveries
 WHERE deliveries.update_row_id = updates.id
   AND deliveries.state = 'delivered'
   AND updates.consumed_at IS NULL;

UPDATE updates AS updates
   SET consumed_at = now()
  FROM bot_update_state AS state
 WHERE state.bot_id = updates.bot_id
   AND state.confirmed_through IS NOT NULL
   AND updates.update_id <= state.confirmed_through
   AND updates.consumed_at IS NULL;

CREATE INDEX IF NOT EXISTS updates_poll_ready_idx
    ON updates (bot_id, update_id)
    WHERE consumed_at IS NULL;
