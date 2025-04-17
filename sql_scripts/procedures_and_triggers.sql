-- Phase 2: Order Placement and Inventory Management
/*
PROCEDURES and TRIGGERS were used to handle order placement, restocking, and inventory logging.

PROCEDURES:
- `place_order`: Handles the logic for placing an order, including stock checks, discounts, and inventory updates.
- `restock`: Adds stock for a specific product and ensures the product exists before updating.
- `auto_restock`: Automatically replenishes stock for low-stock products using a predefined view.

TRIGGERS:
- `log_inventory_changes`: Automatically logs inventory changes (e.g., orders or restocks) into the `inventory_logs` table.

KEY ELEMENTS:
- `RAISE EXCEPTION`: Used to throw errors when conditions like insufficient stock or invalid inputs are met.
- `RAISE NOTICE`: used to pass feedback messages for successful operations.
- `FOR LOOP`: Iterates through arrays or records to process multiple items.
- `NEW` and `OLD`: Used in triggers to access the new and old values of a row being updated.
*/

-- Procedure to handle order placement
CREATE OR REPLACE PROCEDURE place_order(
    p_customer_id INT,
    p_products INT[],
    p_quantities INT[]
)
LANGUAGE plpgsql AS $$
DECLARE
    v_order_id INT;
    v_total_amount DECIMAL(12, 2) := 0;
    v_subtotal DECIMAL(12, 2);
    v_discounted_price DECIMAL(12, 2);
    v_index INT;
    v_price DECIMAL(12, 2);
    v_old_stock INT;
BEGIN
    IF array_length(p_products, 1) != array_length(p_quantities, 1) THEN
        RAISE EXCEPTION 'Product and quantity arrays must have the same length';
    END IF;

    -- Validate products and stock
    FOR v_index IN 1..array_length(p_products, 1) LOOP
        IF NOT EXISTS (SELECT 1 FROM products WHERE product_id = p_products[v_index]) THEN
            RAISE EXCEPTION 'Product ID % not found', p_products[v_index];
        END IF;

        SELECT stock_quantity INTO v_old_stock
        FROM products
        WHERE product_id = p_products[v_index];

        IF v_old_stock < p_quantities[v_index] THEN
            RAISE EXCEPTION 'Not enough stock for product ID %. Available: %, Requested: %',
                p_products[v_index], v_old_stock, p_quantities[v_index];
        END IF;
    END LOOP;

    -- Create new order
    INSERT INTO orders (customer_id, order_date, total_amount)
    VALUES (p_customer_id, CURRENT_TIMESTAMP, 0)
    RETURNING order_id INTO v_order_id;

    FOR v_index IN 1..array_length(p_products, 1) LOOP
        -- Get price
        SELECT price INTO v_price
        FROM products
        WHERE product_id = p_products[v_index];

        -- Discounts
        IF p_quantities[v_index] >= 50 THEN
            v_discounted_price := v_price * 0.85;
        ELSIF p_quantities[v_index] >= 20 THEN
            v_discounted_price := v_price * 0.90;
        ELSE
            v_discounted_price := v_price;
        END IF;

        v_subtotal := v_discounted_price * p_quantities[v_index];
        v_total_amount := v_total_amount + v_subtotal;

        -- Order detail
        INSERT INTO order_details (order_id, product_id, quantity, unit_price, subtotal)
        VALUES (v_order_id, p_products[v_index], p_quantities[v_index], v_discounted_price, v_subtotal);

        -- Log before update
        SELECT stock_quantity INTO v_old_stock FROM products WHERE product_id = p_products[v_index];

        -- Update stock
        UPDATE products
        SET stock_quantity = stock_quantity - p_quantities[v_index]
        WHERE product_id = p_products[v_index];

        -- Log change
        INSERT INTO inventory_logs (
            product_id, change_quantity, change_reason, reference_id,
            previous_quantity, new_quantity
        )
        VALUES (
            p_products[v_index],
            -p_quantities[v_index],
            'ORDER',
            v_order_id,
            v_old_stock,
            v_old_stock - p_quantities[v_index]
        );
    END LOOP;

    UPDATE orders
    SET total_amount = v_total_amount
    WHERE order_id = v_order_id;

    RAISE NOTICE 'Order placed. Order ID: %', v_order_id;
END;
$$;


-- Procedure to restock products
CREATE OR REPLACE PROCEDURE restock(
    p_product_id INT
,   p_quantity INT
) 
LANGUAGE plpgsql
AS $$
BEGIN
    -- Ensure quantity is valid
    IF p_quantity <= 0 THEN
        RAISE EXCEPTION 'Quantity must be greater than zero';
    END IF;
    
    -- Check if the product exists
    IF NOT EXISTS (SELECT 1 FROM products WHERE product_id = p_product_id) THEN
        RAISE EXCEPTION 'Product ID % not found', p_product_id;
    END IF;
    
    -- Update stock
    UPDATE products
    SET stock_quantity = stock_quantity + p_quantity
    WHERE product_id = p_product_id;
    
    RAISE NOTICE '% item(s) added to stock for product ID %', p_quantity, p_product_id;
END;
$$;

-- Trigger function to log inventory changes
CREATE OR REPLACE FUNCTION log_inventory_changes()
RETURNS TRIGGER AS $$
DECLARE
    v_change_quantity INT; -- Difference between old and new stock
    v_change_reason TEXT; -- Reason for the stock change
BEGIN
    -- Calculate stock change
    v_change_quantity := NEW.stock_quantity - OLD.stock_quantity;
    
    -- Skip logging if no change
    IF v_change_quantity = 0 THEN
        RETURN NEW;
    END IF;
    
    -- Determine the reason for the change
    IF v_change_quantity > 0 THEN
        v_change_reason := 'REPLENISHMENT';
    ELSE
        v_change_reason := 'ORDER';
    END IF;
    
    -- Log the change
    INSERT INTO inventory_logs (
        product_id
    ,   change_quantity
    ,   change_reason
    ,   previous_quantity
    ,   new_quantity
    )
    VALUES (
        NEW.product_id
    ,   v_change_quantity
    ,   v_change_reason
    ,   OLD.stock_quantity
    ,   NEW.stock_quantity
    );
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to log inventory changes after stock updates
CREATE OR REPLACE TRIGGER inventory_update_trigger
AFTER UPDATE OF stock_quantity ON products
FOR EACH ROW
EXECUTE FUNCTION log_inventory_changes();

-- Procedure to automatically restock low-stock products
CREATE OR REPLACE PROCEDURE auto_restock()
LANGUAGE plpgsql
AS $$
DECLARE
    v_product RECORD; -- Stores product details from the low_stock_products view
BEGIN
    -- Loop through low-stock products
    FOR v_product IN 
        SELECT 
            product_id, 
            reorder_level - stock_quantity AS reorder_quantity
        FROM 
            low_stock_products
    LOOP
        -- Restock to twice the reorder level
        CALL restock(v_product.product_id, v_product.reorder_quantity + v_product.reorder_level);
    END LOOP;

    RAISE NOTICE 'Auto-restock completed for all low-stock products in the low_stock_products view';
END;
$$;