-- P4 public API persistence (migration 002). Additive only: 001 tables
-- are untouched, and every new table is created IF NOT EXISTS so a
-- fresh database and an upgraded P1-P3 database converge.
-- No credentials are stored here beyond the client secret *hash*;
-- plaintext secrets are never persisted anywhere.

CREATE TABLE IF NOT EXISTS client_credentials (
    client_id VARCHAR PRIMARY KEY,
    record_json VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_messages (
    run_id VARCHAR NOT NULL,
    seq BIGINT NOT NULL,
    message_json VARCHAR NOT NULL,
    PRIMARY KEY (run_id, seq)
);

CREATE TABLE IF NOT EXISTS user_profiles (
    profile_id VARCHAR PRIMARY KEY,
    record_json VARCHAR NOT NULL
);
