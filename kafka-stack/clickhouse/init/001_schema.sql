CREATE DATABASE IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.events
(
    id UInt64,
    event_type String,
    payload String,
    received_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (received_at, id);

INSERT INTO app.events (id, event_type, payload)
VALUES
  (1, 'bootstrap', '{"hello":"clickhouse"}'),
  (2, 'bootstrap', '{"seed":1}');
