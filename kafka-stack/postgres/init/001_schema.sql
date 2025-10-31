CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.events (
  id          BIGSERIAL PRIMARY KEY,
  event_type  TEXT        NOT NULL,
  payload     JSONB       NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO app.events (event_type, payload)
VALUES
  ('bootstrap', '{"hello":"postgres"}'::jsonb),
  ('bootstrap', '{"seed":1}'::jsonb);
