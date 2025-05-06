-- Create the database if it doesn’t exist
CREATE DATABASE heartbeat_db;

-- Connect to the database
\c heartbeat_db;

-- Drop and recreate the table for athlete heart rate data
DROP TABLE IF EXISTS ath_heartbeats;
CREATE TABLE ath_heartbeats (
    id SERIAL PRIMARY KEY,
    athlete_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    heart_rate INT NOT NULL,
    activity_status VARCHAR(20)
);

-- Indexes to improve query performance
CREATE INDEX IF NOT EXISTS idx_hr_time ON ath_heartbeats(timestamp);
CREATE INDEX IF NOT EXISTS idx_hr_athlete ON ath_heartbeats(athlete_id);


-- View 1: Recent High Heart Rate Events (> 150 bpm)
CREATE OR REPLACE VIEW vw_recent_high_hr AS
SELECT 
    athlete_id,
    heart_rate,
    timestamp,
    activity_status
FROM ath_heartbeats
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
FROM ath_heartbeats
WHERE heart_rate < 40
ORDER BY timestamp DESC
LIMIT 100;


-- View 3: Rapid Heart Rate Increases (> +40 bpm in < 2 minutes)

CREATE OR REPLACE VIEW vw_rapid_hr_increase AS
WITH sorted_hr AS (
    SELECT 
        athlete_id,
        heart_rate,
        timestamp,
        LAG(heart_rate) OVER (PARTITION BY athlete_id ORDER BY timestamp) AS prev_hr,
        LAG(timestamp) OVER (PARTITION BY athlete_id ORDER BY timestamp) AS prev_time
    FROM ath_heartbeats
),
spike_events AS (
    SELECT 
        athlete_id,
        timestamp,
        prev_time,
        heart_rate,
        prev_hr,
        heart_rate - prev_hr AS hr_delta,
        EXTRACT(EPOCH FROM timestamp - prev_time) AS time_diff_secs,
        activity_status
    FROM sorted_hr
    WHERE prev_hr IS NOT NULL
)
SELECT *
FROM spike_events
WHERE hr_delta >= 40 AND time_diff_secs <= 120
ORDER BY timestamp DESC
LIMIT 100;
