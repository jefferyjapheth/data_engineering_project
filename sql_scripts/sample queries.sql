-- Insert sample products
INSERT INTO products 
VALUES 
    (1, 'Product A', 'Category 1', 5, 100.00, 10),
    (2, 'Product B', 'Category 2', 20, 50.00, 15),
    (3, 'Product C', 'Category 3', 5, 200.00, 5);

-- Select data to verify
SELECT * FROM products;


-- Insert sample customers
INSERT INTO customers 
VALUES 
    (1, 'Godsaves Essuman', 'saves.esse@example.com', '123-456-7890'),
    (2, 'Elias Smith', 'elias.smith@example.com', '987-654-3210'),
    (3, 'Jeffery Japheth', 'jeffery.j@example.com', '555-123-4567');

-- Select data to verify
SELECT * FROM customers;


/*Demonstrating the use of the place_order procedure*/
-- Place an order for customer with ID 1
CALL place_order(
    p_customer_id := 1,
    p_products := ARRAY[1, 2], -- Product IDs
    p_quantities := ARRAY[3, 5] -- Quantities for each product
);

SELECT * FROM products; -- Check the order details for the placed order
SELECT * FROM orders; -- Check the order details for the placed order
SELECT * FROM order_details; -- Check the order details for the placed order
SELECT * FROM inventory_logs; -- Check the inventory logs for the placed order


/*Demonstrating the use of the restock procedure*/
-- Restock product with ID 1 by 20 units
CALL restock(
    p_product_id := 1,
    p_quantity := 10
);

-- Check the inventory logs for the restock action
SELECT * FROM inventory_logs; 


/*
Report Summaries: Provide sample queries that demonstrate how you retrieve order 
summaries and stock insights
*/

SELECT * FROM CUSTOMER_ORDER_SUMMARY;

-- Check the customer order summary
SELECT * FROM LOW_STOCK_PRODUCTS;


-- Check the low stock products
SELECT * FROM CUSTOMER_SPENDING_INSIGHTS;


-- Check the customer spending insights
SELECT * FROM ORDER_SUMMARY;


-- Select data to verify
SELECT * FROM products;
SELECT * FROM customers;
SELECT * FROM orders;
SELECT * FROM order_details;
SELECT * FROM inventory_logs;