-- Phase 3: Monitoring and Reporting
/*
Simple views vs materialized views:
- Simple views are virtual tables that do not store data physically.
- Materialized views are useful for performance optimization,
since they're already stored and since they're already stored
Chose WITH DATA over WITH DATA because i do not intend to 
add any job schedulers.
Keeping it simple for now.   

used COUNT, SUM, and GROUP BY are aggregate functions used to summarize data.

JOINS were used to combine rows from two or more tables
 based on a related column between them.

LEFT JOIN was used to include all records from the left table (customers)
 and matched records from the right table (orders).

created aliases for the tables to make the queries more readable.
e.g: o.order_id instead of orders.order_id  

FUNCTION() was used to create the function get_customer_tier that
 categorizes customers based on their spending.
*/

-- Materialized View for Order Summaries
CREATE MATERIALIZED VIEW order_summary AS
SELECT 
    o.order_id
,   c.customer_id
,   c.name AS customer_name
,   o.order_date
,   o.total_amount
,   COUNT(od.order_detail_id) AS number_of_items
,   SUM(od.quantity) AS total_quantity
FROM 
    orders o
JOIN 
    customers c ON o.customer_id = c.customer_id
JOIN 
    order_details od ON o.order_id = od.order_id
GROUP BY 
    o.order_id, c.customer_id, c.name, o.order_date, o.total_amount
WITH DATA; -- Refreshes the view on demand, not automatically



-- Materialized View for Low Stock Products
CREATE MATERIALIZED VIEW low_stock_products AS
SELECT 
    product_id
,   name
,   category
,   stock_quantity
,   reorder_level
,   (reorder_level - stock_quantity) AS quantity_to_reorder
FROM 
    products
WHERE 
    stock_quantity < reorder_level
ORDER BY 
    (reorder_level - stock_quantity) DESC
WITH DATA;



-- Function to categorize customers based on spending
CREATE OR REPLACE FUNCTION get_customer_tier(p_total_spent DECIMAL)
RETURNS VARCHAR AS $$
BEGIN
    RETURN CASE
        WHEN p_total_spent >= 5000 THEN 'Gold'
        WHEN p_total_spent >= 1500 THEN 'Silver'
        ELSE 'Bronze'
    END;
END;
$$ LANGUAGE plpgsql;



-- Materialized View for Customer Spending Analysis
CREATE MATERIALIZED VIEW customer_spending_analysis AS
SELECT 
    c.customer_id
,   c.name
,   c.email
,   SUM(o.total_amount) AS total_spent
,   COUNT(o.order_id) AS order_count
,   get_customer_tier(SUM(o.total_amount)) AS customer_tier
FROM 
    customers c
LEFT JOIN 
    orders o ON c.customer_id = o.customer_id
GROUP BY 
    c.customer_id, c.name, c.email
ORDER BY 
    total_spent DESC
WITH DATA;