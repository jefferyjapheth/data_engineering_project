-- Create the database (if it doesn’t exist)
CREATE DATABASE heartbeat_db;

-- Connect to the database
\c heartbeat_db;

-- Create the table for athlete heart rate data
CREATE TABLE IF NOT EXISTS ath_heartbeats (
    id SERIAL PRIMARY KEY,  -- Optional auto-increment ID
    athlete_id VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    heart_rate INT NOT NULL
);

-- Add indexes to improve query performance
CREATE INDEX IF NOT EXISTS idx_hr_time ON ath_heartbeats(timestamp);
CREATE INDEX IF NOT EXISTS idx_hr_athlete ON ath_heartbeats(athlete_id);

-- Optional: View to get recent high heart rate events
CREATE OR REPLACE VIEW recent_high_hr AS
SELECT athlete_id, heart_rate, timestamp
FROM ath_heartbeats
WHERE heart_rate > 150
ORDER BY timestamp DESC
LIMIT 100;