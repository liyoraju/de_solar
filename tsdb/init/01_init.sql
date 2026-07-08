-- TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create the solar inverter raw data table
CREATE TABLE IF NOT EXISTS raw_solar (
    time TIMESTAMPTZ NOT NULL,
    device_sn TEXT,
    device_type TEXT,
    device_state INTEGER,
    collection_time BIGINT,
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

-- Convert to hypertable with 1-day chunks
SELECT create_hypertable('raw_solar', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_raw_solar_device_sn ON raw_solar (device_sn);
CREATE INDEX IF NOT EXISTS idx_raw_solar_collection_time ON raw_solar (collection_time);

-- Enable compression for older data
ALTER TABLE raw_solar SET (timescaledb.compress = true);

-- Auto-compress chunks older than 7 days
SELECT add_compression_policy('raw_solar', INTERVAL '7 days', if_not_exists => TRUE);

-- Keep data for 30 days
SELECT add_retention_policy('raw_solar', INTERVAL '30 days', if_not_exists => TRUE);