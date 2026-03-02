-- ================================================
-- PROJECT 2: DELTA COPY (LOOKUP + FILTER)
-- ================================================

-- NOTE: Reuses dbo.sales_orders from Project 1 as source
-- NOTE: control schema already created in Project 1

-- Step 1: Create delta watermark control table (tracks by ID)
CREATE TABLE control.delta_watermark (
    table_name VARCHAR(100) PRIMARY KEY,
    last_id    INT
);

-- Step 2: Insert initial watermark (0 = load everything on first run)
INSERT INTO control.delta_watermark VALUES ('sales_orders', 0);

-- Step 3: Create sink table
CREATE TABLE dbo.sales_orders_delta_sink (
    order_id      INT,
    customer_name VARCHAR(100),
    product       VARCHAR(100),
    quantity      INT,
    unit_price    FLOAT,
    total_amount  FLOAT,
    order_status  VARCHAR(50),
    modified_date DATETIME
);

-- ================================================
-- Step 4: Create stored procedure (run separately)
-- ================================================
CREATE PROCEDURE control.usp_UpdateDeltaWatermark
    @table_name VARCHAR(100),
    @last_id    INT
AS
BEGIN
    UPDATE control.delta_watermark
    SET last_id = @last_id
    WHERE table_name = @table_name;
END;

-- ================================================
-- INCREMENTAL TEST: Insert new records to test
-- ================================================
-- INSERT INTO dbo.sales_orders (customer_name, product, quantity, unit_price, total_amount, order_status, modified_date) VALUES
-- ('Mia Clark',   'Tablet',        1, 349.99, 349.99, 'Shipped',   '2026-03-03 09:00:00'),
-- ('Noah Adams',  'Gaming Mouse',  2,  49.99,  99.98, 'Delivered', '2026-03-03 11:00:00');

-- ================================================
-- RESET: Use these to reset and retest
-- ================================================
-- TRUNCATE TABLE dbo.sales_orders_delta_sink;
-- UPDATE control.delta_watermark SET last_id = 0 WHERE table_name = 'sales_orders';
