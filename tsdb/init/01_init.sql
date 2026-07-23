-- TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create the solar inverter raw data table
CREATE TABLE IF NOT EXISTS raw_solar (
    time TIMESTAMPTZ NOT NULL,
    device_sn TEXT NOT NULL,
    device_type TEXT,
    device_state INTEGER,
    collection_time BIGINT NOT NULL,
    granularity INTEGER,
    source TEXT,
    rated_power DOUBLE PRECISION,
    dc_voltage_pv1 DOUBLE PRECISION,
    dc_voltage_pv2 DOUBLE PRECISION,
    dc_voltage_pv3 DOUBLE PRECISION,
    dc_voltage_pv4 DOUBLE PRECISION,
    dc_voltage_pv5 DOUBLE PRECISION,
    dc_voltage_pv6 DOUBLE PRECISION,
    dc_voltage_pv7 DOUBLE PRECISION,
    dc_voltage_pv8 DOUBLE PRECISION,
    dc_current_pv1 DOUBLE PRECISION,
    dc_current_pv2 DOUBLE PRECISION,
    dc_current_pv3 DOUBLE PRECISION,
    dc_current_pv4 DOUBLE PRECISION,
    dc_current_pv5 DOUBLE PRECISION,
    dc_current_pv6 DOUBLE PRECISION,
    dc_current_pv7 DOUBLE PRECISION,
    dc_current_pv8 DOUBLE PRECISION,
    dc_power_pv1 DOUBLE PRECISION,
    dc_power_pv2 DOUBLE PRECISION,
    dc_power_pv3 DOUBLE PRECISION,
    dc_power_pv4 DOUBLE PRECISION,
    dc_power_pv5 DOUBLE PRECISION,
    dc_power_pv6 DOUBLE PRECISION,
    dc_power_pv7 DOUBLE PRECISION,
    dc_power_pv8 DOUBLE PRECISION,
    ac_voltage_rua DOUBLE PRECISION,
    ac_voltage_svb DOUBLE PRECISION,
    ac_voltage_twc DOUBLE PRECISION,
    ac_current_rua DOUBLE PRECISION,
    ac_current_svb DOUBLE PRECISION,
    ac_current_twc DOUBLE PRECISION,
    ac_output_frequency_r DOUBLE PRECISION,
    total_active_ac_output_power DOUBLE PRECISION,
    ab_line_voltage DOUBLE PRECISION,
    bc_line_voltage DOUBLE PRECISION,
    ac_line_voltage DOUBLE PRECISION,
    total_active_production DOUBLE PRECISION,
    daily_active_production DOUBLE PRECISION,
    inverter_output_power_l1 DOUBLE PRECISION,
    inverter_output_power_l2 DOUBLE PRECISION,
    inverter_output_power_l3 DOUBLE PRECISION,
    total_grid_feed_in DOUBLE PRECISION,
    total_energy_purchased DOUBLE PRECISION,
    total_consumption_power DOUBLE PRECISION,
    total_consumption DOUBLE PRECISION
);
-- Drop old constraints and indexes if resetting
ALTER TABLE raw_solar DROP CONSTRAINT IF EXISTS raw_solar_pkey;
ALTER TABLE raw_solar DROP CONSTRAINT IF EXISTS idx_raw_solar_unique;
DROP INDEX IF EXISTS raw_solar_unique;

ALTER TABLE raw_solar 
ADD CONSTRAINT raw_solar_pkey 
PRIMARY KEY (device_sn, collection_time, time);

-- Convert to hypertable with 1-day chunks
SELECT create_hypertable('raw_solar', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');

-- Create secondary indexes for quick lookups
-- Note: An index on (device_sn, collection_time, time) is already automatically created by the primary key!
CREATE INDEX IF NOT EXISTS idx_raw_solar_device_sn ON raw_solar (device_sn);

-- Enable compression for older data
ALTER TABLE raw_solar SET (timescaledb.compress = true);

-- Auto-compress chunks older than 7 days
SELECT add_compression_policy('raw_solar', INTERVAL '7 days', if_not_exists => TRUE);

-- Keep data for 7 days (Note: Retention policy will delete chunks older than 7 days, 
-- which means data will be dropped immediately after it hits your compression policy threshold)
SELECT add_retention_policy('raw_solar', INTERVAL '7 days', if_not_exists => TRUE);
-- ── raw_solar_h2 (daily aggregates) ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_solar_h2 (
    time TIMESTAMPTZ NOT NULL,
    device_sn TEXT NOT NULL,
    device_type TEXT,
    device_state INTEGER,
    collection_time BIGINT NOT NULL,
    granularity INTEGER,
    source TEXT,
    total_active_production DOUBLE PRECISION,
    total_grid_feed_in DOUBLE PRECISION,
    total_consumption DOUBLE PRECISION,
    total_energy_purchased DOUBLE PRECISION
);

ALTER TABLE raw_solar_h2
ADD CONSTRAINT raw_solar_h2_pkey
PRIMARY KEY (device_sn, collection_time, time);

SELECT create_hypertable('raw_solar_h2', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');
CREATE INDEX IF NOT EXISTS idx_raw_solar_h2_device_sn ON raw_solar_h2 (device_sn);
ALTER TABLE raw_solar_h2 SET (timescaledb.compress = true);
SELECT add_compression_policy('raw_solar_h2', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('raw_solar_h2', INTERVAL '365 days', if_not_exists => TRUE);


-- ── raw_solar_h3 (monthly aggregates) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_solar_h3 (
    time TIMESTAMPTZ NOT NULL,
    device_sn TEXT NOT NULL,
    device_type TEXT,
    device_state INTEGER,
    collection_time BIGINT NOT NULL,
    granularity INTEGER,
    source TEXT,
    total_active_production DOUBLE PRECISION,
    total_grid_feed_in DOUBLE PRECISION,
    total_consumption DOUBLE PRECISION,
    total_energy_purchased DOUBLE PRECISION
);

ALTER TABLE raw_solar_h3
ADD CONSTRAINT raw_solar_h3_pkey
PRIMARY KEY (device_sn, collection_time, time);

SELECT create_hypertable('raw_solar_h3', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 year');
CREATE INDEX IF NOT EXISTS idx_raw_solar_h3_device_sn ON raw_solar_h3 (device_sn);
ALTER TABLE raw_solar_h3 SET (timescaledb.compress = true);
SELECT add_compression_policy('raw_solar_h3', INTERVAL '1 year', if_not_exists => TRUE);
SELECT add_retention_policy('raw_solar_h3', INTERVAL '5 years', if_not_exists => TRUE);


-- ── raw_solar_h4 (yearly aggregates) ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_solar_h4 (
    time TIMESTAMPTZ NOT NULL,
    device_sn TEXT NOT NULL,
    device_type TEXT,
    device_state INTEGER,
    collection_time BIGINT NOT NULL,
    granularity INTEGER,
    source TEXT,
    total_active_production DOUBLE PRECISION,
    total_grid_feed_in DOUBLE PRECISION,
    total_consumption DOUBLE PRECISION,
    total_energy_purchased DOUBLE PRECISION
);

ALTER TABLE raw_solar_h4
ADD CONSTRAINT raw_solar_h4_pkey
PRIMARY KEY (device_sn, collection_time, time);

SELECT create_hypertable('raw_solar_h4', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '10 years');
CREATE INDEX IF NOT EXISTS idx_raw_solar_h4_device_sn ON raw_solar_h4 (device_sn);
ALTER TABLE raw_solar_h4 SET (timescaledb.compress = false);
SELECT add_retention_policy('raw_solar_h4', INTERVAL '20 years', if_not_exists => TRUE);
