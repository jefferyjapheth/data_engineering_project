-- Create the database if it doesn’t exist
CREATE DATABASE heartrate_db;

-- Connect to the database
\c heartrate_db;

-- Drop and recreate the table for athlete heart rate data
DROP TABLE IF EXISTS athlete_heartrates;
-- Create the database if it doesn’t exist (for use outside psql)
-- You can skip this in Docker setup if DB is created via environment vars

-- \c heartrate_db -- (use only if executing interactively)

-- Drop and recreate the table for athlete heart rate data
DROP TABLE IF EXISTS athlete_heartrates;
CREATE TABLE athlete_heartrates (
    id SERIAL PRIMARY KEY,
    athlete_id VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    heart_rate SMALLINT NOT NULL CHECK (heart_rate >= 0),
    activity_status VARCHAR(20)
);

-- Indexes to improve query performance
CREATE INDEX IF NOT EXISTS idx_hr_timestamp ON athlete_heartrates(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_hr_athlete_id ON athlete_heartrates(athlete_id);
CREATE INDEX IF NOT EXISTS idx_hr_value ON athlete_heartrates(heart_rate);

-- Drop old views if they exist
DROP VIEW IF EXISTS vw_recent_high_hr;
DROP VIEW IF EXISTS vw_abnormal_low_hr;

-- View: Summary of high heart rate events (> 150 bpm)
CREATE VIEW vw_recent_high_hr AS
SELECT 
    athlete_id,
    MAX(heart_rate) AS max_heart_rate,
    MIN(heart_rate) AS min_heart_rate,
    ROUND(AVG(heart_rate)::NUMERIC, 2) AS avg_heart_rate,
    COUNT(*) AS total_events,
    MAX(timestamp) AS last_event_timestamp
FROM athlete_heartrates
WHERE heart_rate > 150
GROUP BY athlete_id;

-- View: Summary of abnormally low heart rate events (< 40 bpm)
CREATE VIEW vw_abnormal_low_hr AS
SELECT 
    athlete_id,
    MAX(heart_rate) AS max_heart_rate,
    MIN(heart_rate) AS min_heart_rate,
    ROUND(AVG(heart_rate)::NUMERIC, 2) AS avg_heart_rate,
    COUNT(*) AS total_events,
    MAX(timestamp) AS last_event_timestamp
FROM athlete_heartrates
WHERE heart_rate < 40
GROUP BY athlete_id;
