  -- Create database (if it doesn’t exist)
  CREATE DATABASE ecommerce_db;

  -- Connect to it
  \c ecommerce_db;

  -- Create the events table with event_key as UUID
  CREATE TABLE IF NOT EXISTS ecommerce_events (
    event_key UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL
  );

  -- Indexes for common queries
  CREATE INDEX IF NOT EXISTS idx_ecom_time ON ecommerce_events(event_time);
  CREATE INDEX IF NOT EXISTS idx_ecom_user ON ecommerce_events(user_id);
  CREATE INDEX IF NOT EXISTS idx_ecom_type ON ecommerce_events(event_type);

  -- Convenience view of recent purchases
  CREATE OR REPLACE VIEW recent_purchases AS
  SELECT user_id, product_name, category, price, quantity, event_time
  FROM ecommerce_events
  WHERE event_type = 'purchase'
  ORDER BY event_time DESC
  LIMIT 100;
