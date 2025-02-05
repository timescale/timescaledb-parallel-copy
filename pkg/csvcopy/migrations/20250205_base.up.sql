CREATE TABLE timescaledb_parallel_copy (
    file_id TEXT NOT NULL,
    start_row BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    byte_offset BIGINT NOT NULL,
    byte_len BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    state TEXT NOT NULL ,
    failure_reason TEXT DEFAULT NULL,
    UNIQUE (file_id, start_row)
);
