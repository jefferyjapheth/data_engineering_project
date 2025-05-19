-- Create a dedicated schema to flight analytics 
CREATE SCHEMA IF NOT EXISTS flight_analytics;

-- Drop and recreate the cleaned flight data table
DROP TABLE IF EXISTS flight_analytics.flight_prices_cleaned;
CREATE TABLE flight_analytics.flight_prices_cleaned (
    airline VARCHAR,
    source VARCHAR,
    source_name VARCHAR,
    destination VARCHAR,
    destination_name VARCHAR,
    departure_timestamp TIMESTAMPTZ,
    arrival_timestamp TIMESTAMPTZ,
    duration_hours DOUBLE PRECISION,
    stopovers VARCHAR,
    aircraft_type VARCHAR,
    class VARCHAR,
    booking_source VARCHAR,
    base_fare_bdt DOUBLE PRECISION,
    tax_surcharge_bdt DOUBLE PRECISION,
    total_fare_bdt DOUBLE PRECISION,
    seasonality VARCHAR,
    days_before_departure INT
);

-- Drop and recreate KPI tables

-- KPI 1: Average fare by airline
DROP TABLE IF EXISTS flight_analytics.kpi_avg_fare_by_airline;
CREATE TABLE flight_analytics.kpi_avg_fare_by_airline (
    airline VARCHAR,
    avg_total_fare DOUBLE PRECISION
);

-- KPI 2: Seasonal fare variation
DROP TABLE IF EXISTS flight_analytics.kpi_seasonal_variation;
CREATE TABLE flight_analytics.kpi_seasonal_variation (
    seasonality VARCHAR,
    avg_fare DOUBLE PRECISION
);

-- KPI 3: Booking count by airline
DROP TABLE IF EXISTS flight_analytics.kpi_booking_count;
CREATE TABLE flight_analytics.kpi_booking_count (
    airline VARCHAR,
    booking_count BIGINT
);

-- KPI 4: Popular routes
DROP TABLE IF EXISTS flight_analytics.kpi_popular_routes;
CREATE TABLE flight_analytics.kpi_popular_routes (
    source VARCHAR,
    destination VARCHAR,
    route_count BIGINT
);

-- Indexing
CREATE INDEX IF NOT EXISTS idx_fp_airline ON flight_analytics.flight_prices_cleaned(airline);
CREATE INDEX IF NOT EXISTS idx_fp_departure ON flight_analytics.flight_prices_cleaned(departure_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fp_route ON flight_analytics.flight_prices_cleaned(source, destination);
