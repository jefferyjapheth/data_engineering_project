-- Create the database if it doesn’t exist
CREATE DATABASE heartrate_db;

-- Connect to the database
\c heartrate_db;

-- Drop and recreate the table for athlete heart rate data
DROP TABLE IF EXISTS athlete_heartrates;
CREATE TABLE athlete_heartrates (
    id SERIAL PRIMARY KEY,
    athlete_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    heart_rate INT NOT NULL,
    activity_status VARCHAR(20)
);

-- Indexes to improve query performance
CREATE INDEX IF NOT EXISTS idx_hr_time ON athlete_heartrates(timestamp);
CREATE INDEX IF NOT EXISTS idx_hr_athlete ON athlete_heartrates(athlete_id);


-- View 1: Recent High Heart Rate Events (> 150 bpm)
CREATE OR REPLACE VIEW vw_recent_high_hr AS
SELECT 
    athlete_id,
    heart_rate,
    timestamp,
    activity_status
FROM athlete_heartrates
WHERE heart_rate > 150
ORDER BY timestamp DESC
LIMIT 100;


-- View 2: Abnormally Low Heart Rate (< 40 bpm)
CREATE OR REPLACE VIEW vw_abnormal_low_hr AS
SELECT 
    athlete_id,
    heart_rate,
    timestamp,
    activity_status
FROM athlete_heartrates
WHERE heart_rate < 40
ORDER BY timestamp DESC
LIMIT 100;





-- View 1: Recent High Heart Rate Events (> 150 bpm)
CREATE OR REPLACE VIEW vw_recent_high_hr AS
SELECT 
    athlete_id,
    MAX(heart_rate) AS max_heart_rate,
    MIN(heart_rate) AS min_heart_rate,
    AVG(heart_rate) AS avg_heart_rate,
    COUNT(*) AS total_events,
    MAX(timestamp) AS last_event_timestamp
FROM athlete_heartrates
WHERE heart_rate > 150
GROUP BY athlete_id
ORDER BY last_event_timestamp DESC
LIMIT 100;

-- View 2: Abnormally Low Heart Rate (< 40 bpm)
CREATE OR REPLACE VIEW vw_abnormal_low_hr AS
SELECT 
    athlete_id,
    MAX(heart_rate) AS max_heart_rate,
    MIN(heart_rate) AS min_heart_rate,
    AVG(heart_rate) AS avg_heart_rate,
    COUNT(*) AS total_events,
    MAX(timestamp) AS last_event_timestamp
FROM athlete_heartrates
WHERE heart_rate < 40
GROUP BY athlete_id
ORDER BY last_event_timestamp DESC
LIMIT 100;

-- Drop the existing view if it exists
DROP VIEW IF EXISTS vw_recent_high_hr;

-- Now create the view again
CREATE VIEW vw_recent_high_hr AS
SELECT 
    athlete_id,
    MAX(heart_rate) AS max_heart_rate,
    MIN(heart_rate) AS min_heart_rate,
    AVG(heart_rate) AS avg_heart_rate,
    COUNT(*) AS total_events,
    MAX(timestamp) AS last_event_timestamp
FROM athlete_heartrates
WHERE heart_rate > 150
GROUP BY athlete_id
ORDER BY last_event_timestamp DESC
LIMIT 100;

-- Drop the existing view if it exists
DROP VIEW IF EXISTS vw_abnormal_low_hr;

-- Now create the view again
CREATE VIEW vw_abnormal_low_hr AS
SELECT 
    athlete_id,
    MAX(heart_rate) AS max_heart_rate,   -- Alias for the highest heart rate
    MIN(heart_rate) AS min_heart_rate,   -- Alias for the lowest heart rate
    AVG(heart_rate) AS avg_heart_rate,   -- Alias for the average heart rate
    COUNT(*) AS total_events,            -- Alias for total number of events
    MAX(timestamp) AS last_event_timestamp  -- Alias for the most recent event timestamp
FROM athlete_heartrates
WHERE heart_rate < 40
GROUP BY athlete_id
ORDER BY last_event_timestamp DESC
LIMIT 100;
