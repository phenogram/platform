ALTER TABLE conversations ADD COLUMN expires_at TIMESTAMPTZ;
ALTER TABLE api_calls ADD COLUMN expires_at TIMESTAMPTZ;
ALTER TABLE audit_log ADD COLUMN expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '365 days');

-- Account-scoped audit records have no bot plan to inherit. Keep a bounded
-- one-year operational trail instead of allowing them to grow forever.
UPDATE audit_log
   SET expires_at = created_at + interval '365 days'
 WHERE bot_id IS NULL;

UPDATE conversations AS conversations
   SET expires_at = conversations.last_update_at + make_interval(days => plans.retention_days)
  FROM bots
  JOIN memberships ON memberships.user_id = bots.user_id
  JOIN plan_definitions AS plans ON plans.id = memberships.plan_id
 WHERE bots.id = conversations.bot_id;

UPDATE api_calls AS calls
   SET expires_at = calls.created_at + make_interval(days => plans.retention_days)
  FROM bots
  JOIN memberships ON memberships.user_id = bots.user_id
  JOIN plan_definitions AS plans ON plans.id = memberships.plan_id
 WHERE bots.id = calls.bot_id;

UPDATE audit_log AS logs
   SET expires_at = logs.created_at + make_interval(days => plans.retention_days)
  FROM bots
  JOIN memberships ON memberships.user_id = bots.user_id
  JOIN plan_definitions AS plans ON plans.id = memberships.plan_id
 WHERE bots.id = logs.bot_id;

ALTER TABLE conversations ALTER COLUMN expires_at SET NOT NULL;
ALTER TABLE api_calls ALTER COLUMN expires_at SET NOT NULL;

CREATE INDEX conversations_expiry_idx ON conversations (expires_at);
CREATE INDEX api_calls_expiry_idx ON api_calls (expires_at);
CREATE INDEX audit_log_expiry_idx ON audit_log (expires_at) WHERE expires_at IS NOT NULL;
