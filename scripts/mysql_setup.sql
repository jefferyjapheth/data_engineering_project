CREATE TABLE IF NOT EXISTS staging_flight_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    origin VARCHAR(10),
    destination VARCHAR(10),
    airline VARCHAR(50),
    departure_date DATE,
    return_date DATE,
    price DECIMAL(10, 2)
);
