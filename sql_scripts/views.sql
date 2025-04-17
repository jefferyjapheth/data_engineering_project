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

aggregate functions like COUNT and SUM were used to summarize data.
*/


-- Materialized View for Customer Order Summaries
/*
This view summarizes customer orders by grouping them based on their unique email addresses.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_order_summary AS
SELECT 
    c.email AS customer_email -- Grouping by unique email
,   COUNT(o.order_id) AS total_orders  -- Total number of orders placed by the customer
,   SUM(o.total_amount) AS total_spent  -- Total amount spent by the customer
,   SUM(od.quantity) AS total_items_ordered -- Total quantity of items ordered by the customer
FROM 
    customers c
JOIN 
    orders o ON c.customer_id = o.customer_id
JOIN 
    order_details od ON o.order_id = od.order_id
GROUP BY 
    c.email -- Grouping by unique customer email
WITH DATA; -- Refreshes the view on demand, not automatically


-- Materialized View for Low Stock Products
/*
This view identifies products that are low in stock and need to be reordered. 
It has an offset of +2 for practical use case scenario sake .
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS low_stock_products AS
SELECT 
    product_id
,   name
,   category
,   stock_quantity
,   reorder_level
FROM 
    products
WHERE 
    stock_quantity <= reorder_level + 2 -- Include products with stock quantity lower or within 2 units above reorder level
ORDER BY 
    stock_quantity ASC -- Order by stock quantity in ascending order
WITH DATA;



-- Categorize customers based on spending
/*
This view categorizes customers into tiers based on their total spending.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS customer_spending_insights AS
SELECT 
    c.customer_id
,   c.name AS customer_name
,   c.email AS customer_email
,   SUM(o.total_amount) AS total_spent -- Total amount spent by the customer
,   COUNT(o.order_id) AS total_orders -- Total number of orders placed by the customer
,   CASE
        WHEN SUM(o.total_amount) >= 5000 THEN 'Gold'
        WHEN SUM(o.total_amount) >= 1500 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_tier -- Categorize customers into tiers
FROM 
    customers c
LEFT JOIN 
    orders o ON c.customer_id = o.customer_id
GROUP BY 
    c.customer_id, c.name, c.email -- Group by customer details
ORDER BY 
    total_spent DESC -- Order by total spending in descending order
WITH DATA;



-- Materialized View for Order Summary Partitioned by Month
/*
This view summarizes orders by month and customer. Ordered by the most recent month first.
*/
CREATE MATERIALIZED VIEW IF NOT EXISTS order_summary AS
SELECT 
    c.name AS customer_name -- Customer's name
,   DATE_TRUNC('month', o.order_date) AS order_month -- Truncate order date to the first day of the month
,   SUM(o.total_amount) AS total_order_amount -- Total amount for all orders in the month
,   SUM(od.quantity) AS total_items -- Total number of items in the month
FROM 
    customers c
JOIN 
    orders o ON c.customer_id = o.customer_id
JOIN 
    order_details od ON o.order_id = od.order_id
GROUP BY 
    c.name, DATE_TRUNC('month', o.order_date) -- Grouping by customer name and order month
ORDER BY 
    order_month DESC -- Order by the most recent month first
WITH DATA; -- Refreshes the view on demand