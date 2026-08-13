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
    SELECT count(*) INTO current_bots FROM bots WHERE user_id = NEW.user_id;
    IF current_bots >= allowed_bots THEN
        RAISE EXCEPTION 'bot plan limit reached' USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
