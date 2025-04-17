-- Phase 4: Stock Replenishment and Automation
-- Function to replenish stock
CREATE OR REPLACE PROCEDURE replenish_stock(
    p_product_id INT
,   p_quantity INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_stock INT;
BEGIN
    -- Get current stock
    SELECT stock_quantity INTO v_current_stock
    FROM products
    WHERE product_id = p_product_id;
    
    -- Update stock
    UPDATE products
    SET stock_quantity = stock_quantity + p_quantity
    WHERE product_id = p_product_id;
    
    -- Log inventory change
    INSERT INTO inventory_logs (
        product_id, 
        change_quantity, 
        change_reason, 
        previous_quantity, 
        new_quantity
    )
    VALUES (
        p_product_id, 
        p_quantity, 
        'REPLENISHMENT', 
        v_current_stock, 
        v_current_stock + p_quantity
    );
    
    COMMIT;
	RAISE NOTICE '% item(s) added to stock for product ID %', p_quantity, p_product_id;
END;
$$;


-- Procedure to automatically reorder low stock products
CREATE OR REPLACE PROCEDURE auto_replenish_low_stock()
LANGUAGE plpgsql
AS $$
DECLARE
    v_product RECORD;
BEGIN
    FOR v_product IN 
        SELECT 
            product_id, 
            reorder_level - stock_quantity AS reorder_quantity
        FROM 
            products
        WHERE 
            stock_quantity < reorder_level
    LOOP
        -- Standard reorder amount: enough to get back to twice the reorder level
        CALL replenish_stock(v_product.product_id, v_product.reorder_quantity + v_product.reorder_level);
    END LOOP;
	RAISE NOTICE 'Stock auto-replenished for product ID %', p_product_id;
END;
$$;