-- Phase 1: Database Design and Schema Implementation
-- Create Products table
CREATE  TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price > 0),
    stock_quantity INT NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    reorder_level INT NOT NULL DEFAULT 10 CHECK (reorder_level > 0)
);

-- Customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY
,	name VARCHAR(100) NOT NULL
,	email VARCHAR(100) UNIQUE NOT NULL
,	phone VARCHAR(20)
);

-- Orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY
,   customer_id INT NOT NULL REFERENCES customers(customer_id)
,	order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
,   total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0
);

-- Order Details table
CREATE TABLE order_details (
    order_detail_id SERIAL PRIMARY KEY
,   order_id INT NOT NULL REFERENCES orders(order_id)
,   product_id INT NOT NULL REFERENCES products(product_id)
,   quantity INT NOT NULL CHECK (quantity > 0)
,   unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0)
,    subtotal DECIMAL(12, 2) NOT NULL
);

-- Inventory Logs table
CREATE TABLE inventory_logs (
    log_id SERIAL PRIMARY KEY
,   product_id INT NOT NULL REFERENCES products(product_id)
,   change_quantity INT NOT NULL
,   change_reason VARCHAR(50) NOT NULL
,   reference_id INT	-- order_id or replenishment_id
,   previous_quantity INT NOT NULL
,   new_quantity INT NOT NULL
,   change_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

