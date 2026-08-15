-- Native data-plane observations and explicit Bot View sends can see the same
-- successful Telegram Message. Keep exactly one timeline row, preferring the
-- audited operator attribution when historical duplicates already exist.
WITH ranked AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY bot_id, chat_id, telegram_message_id
               ORDER BY (source = 'bot_view') DESC, (user_id IS NOT NULL) DESC, id
           ) AS duplicate_rank
      FROM outbound_messages
     WHERE telegram_message_id IS NOT NULL
)
DELETE FROM outbound_messages
 USING ranked
 WHERE outbound_messages.id = ranked.id
   AND ranked.duplicate_rank > 1;

CREATE UNIQUE INDEX outbound_messages_telegram_identity_idx
    ON outbound_messages (bot_id, chat_id, telegram_message_id)
    WHERE telegram_message_id IS NOT NULL;
