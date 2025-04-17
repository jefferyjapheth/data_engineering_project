-- Phase 1: Database Design and Schema Implementation
/*CREATE TABLE (); was used as a data definition language(DDL) 
which helped in creating the tables 

CONSTRAINTS were used to enforce data integrity |

SERIAL is a constraint that auto-increments-
it helped in indexing the tables as a constraint for the respective columns

NOT NUll ensures the column has no empty/missing values

PRIMARY KEY helps dedicate a column as the primary key of the table

INT ensures the values for the column are integers

DECIMAL ensures the values for the column are decimal numbers with
DECIMAL(10, 2) meaning 10 digits in total with 2 after the decimal point

CHECK ensures the value being added conforms to the conditions stated

DEFAULT ensures the value stated is used whenever there's no value being inserted

UNIQUE ensures no two values in a column are the same 

TIMESTAMP sets the data type of the column to date and time

VARCHAR ensures the values of the column are variable and can be set to a specific length
 
REFERENCES helps assign a column as a foreign key for another column
-this creates relationships between different tables

*/

CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY 
,   name VARCHAR(100) NOT NULL
,   category VARCHAR(50) NOT NULL
,   price DECIMAL(10, 2) NOT NULL CHECK (price > 0)
,   stock_quantity INT NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0)
,   reorder_level INT NOT NULL DEFAULT 10 CHECK (reorder_level > 0)
);

-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY
,	name VARCHAR(100) NOT NULL
,	email VARCHAR(100) UNIQUE NOT NULL
,	phone VARCHAR(20)
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY
,   customer_id INT NOT NULL REFERENCES customers(customer_id)
,	order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
,   total_amount DECIMAL(12, 2) NOT NULL DEFAULT 0
);

-- Order Details table
CREATE TABLE IF NOT EXISTS order_details (
    order_detail_id SERIAL PRIMARY KEY
,   order_id INT NOT NULL REFERENCES orders(order_id)
,   product_id INT NOT NULL REFERENCES products(product_id)
,   quantity INT NOT NULL CHECK (quantity > 0)
,   unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0)
,   subtotal DECIMAL(12, 2) NOT NULL
);

-- Inventory Logs table
CREATE TABLE IF NOT EXISTS inventory_logs (
    log_id SERIAL PRIMARY KEY
,   product_id INT NOT NULL REFERENCES products(product_id)
,   change_quantity INT NOT NULL
,   change_reason VARCHAR(50) NOT NULL
,   reference_id INT NOT NULL DEFAULT 0	-- this represents order_id or replenishment_id
,   previous_quantity INT NOT NULL
,   new_quantity INT NOT NULL
,   change_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

/*
CREATE INDEX (); was used to create an index on the specified column
-an index is a data structure that improves the speed of data retrieval operations on a database table
*/

-- Indexes for performance optimization
-- Creating indexes on commonly queried fields/columns to speed up searches
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_details_order_id ON order_details(order_id);
CREATE INDEX IF NOT EXISTS idx_order_details_product_id ON order_details(product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_logs_product_id ON inventory_logs(product_id);


