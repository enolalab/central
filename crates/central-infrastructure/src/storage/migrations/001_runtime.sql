CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS runs (
    id VARCHAR PRIMARY KEY,
    revision BIGINT NOT NULL,
    snapshot_json VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    run_id VARCHAR NOT NULL,
    seq BIGINT NOT NULL,
    event_json VARCHAR NOT NULL,
    PRIMARY KEY (run_id, seq)
);

CREATE TABLE IF NOT EXISTS outbox (
    id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    intent_json VARCHAR NOT NULL,
    status VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS command_receipts (
    actor_id VARCHAR NOT NULL,
    command_id VARCHAR NOT NULL,
    payload_hash VARCHAR NOT NULL,
    receipt_json VARCHAR NOT NULL,
    PRIMARY KEY (actor_id, command_id)
);
