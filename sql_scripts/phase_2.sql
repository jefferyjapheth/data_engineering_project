-- Phase 2: Order Placement and Inventory Management
/*
A PROCEDURE call  was used to implement the logic for placing an order,
 which includes checking stock levels, calculating totals, and updating inventory.

The procedure takes customer ID, product IDs, and quantities as input parameters.
It first creates a new order, then iterates through the products to check stock levels and calculate the total amount.

PRECONDITIONS for the procedure:
- The customer ID must exist in the customers table.
- The product IDs must exist in the products table.
- The quantities must be positive integers.
- The stock quantity for each product must be sufficient to fulfill the order.

PROCEDURE is the keyword used to define the procedure.
LANGUAGE specifies the programming language(plpgsql) used for the procedure.
AS $$ indicates the start of the procedure body.
DECLARE is used to declare variables within the procedure.
:= is used to assign values to variables
BEGIN marks the start of the logic implementations.
RAISE EXCEPTION is used to raise an error if stock is insufficient.
COMMIT is used to save the changes made by the procedure when the transaction is successful.    
END; marks the end of the procedure.

FOR v_index IN 1..array_length(p_products, 1) 
 is used to iterate through the product IDs and quantities.
*/
-- Procedure call for processing a new order
CREATE OR REPLACE PROCEDURE place_order(
    p_customer_id INT
,   p_products INT[]
,   p_quantities INT[]
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_order_id INT
;   v_product_record RECORD
;   v_total_amount DECIMAL(12, 2) := 0
;   v_subtotal DECIMAL(12, 2)
;   v_current_stock INT
;   v_index INT
;
BEGIN
    -- Create a new order
    INSERT INTO orders 
    VALUES (p_customer_id, CURRENT_TIMESTAMP, 0)
    RETURNING order_id INTO v_order_id;
    
    -- Process each product in the order
    FOR v_index IN 1..array_length(p_products, 1) LOOP
        -- Get product information
        SELECT product_id, price, stock_quantity INTO v_product_record
        FROM products
        WHERE product_id = p_products[v_index];
        
        -- Check if there's enough stock
        IF v_product_record.stock_quantity < p_quantities[v_index] THEN
            RAISE EXCEPTION 'Not enough stock for product ID %', p_products[v_index];
        END IF;
        
        -- Calculate subtotal for this product
        v_subtotal := v_product_record.price * p_quantities[v_index];
        v_total_amount := v_total_amount + v_subtotal;
        
        -- Insert order detail
        INSERT INTO order_details 
        VALUES (v_order_id, p_products[v_index], p_quantities[v_index], v_product_record.price, v_subtotal);
        
        -- Update product stock
        v_current_stock := v_product_record.stock_quantity;
        UPDATE products
        SET stock_quantity = stock_quantity - p_quantities[v_index]
        WHERE product_id = p_products[v_index];
        RAISE NOTICE 'Order successfully placed for customer ID %',p_customer_id;
        -- This marks the end of the order placements logic

        --combined inventory tracking and order placement logic for effiency

        -- Tracking inventory changes
        INSERT INTO inventory_logs 
        VALUES (
            p_products[v_index]
		,   -p_quantities[v_index]
		,   'ORDER'
		,   v_order_id
		,   v_current_stock
		,   v_current_stock - p_quantities[v_index]
        );
		
    END LOOP;
    
    -- Update order total
    UPDATE orders
    SET total_amount = v_total_amount
    WHERE order_id = v_order_id;
     -- This marks the end of the tracking inventory logic

    COMMIT;
	
	RAISE NOTICE 'Inventory log updated for order ID %',order_id;
END;
$$;