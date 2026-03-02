-- ================================================
-- PROJECT 3: CHANGE DATA CAPTURE (CDC)
-- ================================================

-- NOTE: CDC requires Standard S3 tier or higher on Azure SQL
-- NOTE: control schema and dbo.sales_orders already created in Project 1

-- ================================================
-- Step 1: Enable CDC on the database (run separately)
-- ================================================
EXEC sys.sp_cdc_enable_db;

-- Verify CDC is enabled
SELECT name, is_cdc_enabled
FROM sys.databases
WHERE name = DB_NAME();

-- ================================================
-- Step 2: Enable CDC on the source table (run separately)
-- ================================================
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'sales_orders',
    @role_name     = NULL;

-- Verify CDC is enabled on the table
SELECT name, is_tracked_by_cdc
FROM sys.tables
WHERE name = 'sales_orders';

-- ================================================
-- Step 3: Create CDC watermark control table
-- ================================================
CREATE TABLE control.cdc_watermark (
    table_name VARCHAR(100) PRIMARY KEY,
    last_lsn   BINARY(10)
);

-- Initialize with minimum LSN
INSERT INTO control.cdc_watermark (table_name, last_lsn)
VALUES ('sales_orders', sys.fn_cdc_get_min_lsn('dbo_sales_orders'));

-- Step 4: Create LSN staging table
-- (Used to convert binary LSN to string for ADF compatibility)
CREATE TABLE control.cdc_lsn_stage (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    lsn_value  VARCHAR(100),
    created_at DATETIME DEFAULT GETDATE()
);

-- Step 5: Create sink table
CREATE TABLE dbo.sales_orders_cdc_sink (
    order_id      INT,
    customer_name VARCHAR(100),
    product       VARCHAR(100),
    quantity      INT,
    unit_price    FLOAT,
    total_amount  FLOAT,
    order_status  VARCHAR(50),
    modified_date DATETIME,
    cdc_operation VARCHAR(10)
);

-- ================================================
-- Step 6: Create stored procedure (run separately)
-- ================================================
CREATE PROCEDURE control.usp_UpdateCDCWatermark
    @table_name VARCHAR(100),
    @last_lsn   VARCHAR(100)
AS
BEGIN
    UPDATE control.cdc_watermark
    SET last_lsn = CONVERT(BINARY(10), @last_lsn, 1)
    WHERE table_name = @table_name;
END;

-- ================================================
-- INCREMENTAL TEST: Insert, Update, Delete to test CDC
-- ================================================
-- INSERT
-- INSERT INTO dbo.sales_orders (customer_name, product, quantity, unit_price, total_amount, order_status, modified_date) VALUES
-- ('Oliver Brown', 'Smart Speaker', 1, 79.99, 79.99, 'Delivered', GETDATE());

-- UPDATE
-- UPDATE dbo.sales_orders
-- SET order_status = 'Returned', modified_date = GETDATE()
-- WHERE order_id = 15;

-- DELETE
-- DELETE FROM dbo.sales_orders
-- WHERE order_id = 15;

-- ================================================
-- RESET: Use these to reset and retest
-- ================================================
-- TRUNCATE TABLE dbo.sales_orders_cdc_sink;
-- TRUNCATE TABLE control.cdc_lsn_stage;
-- UPDATE control.cdc_watermark
-- SET last_lsn = sys.fn_cdc_get_min_lsn('dbo_sales_orders')
-- WHERE table_name = 'sales_orders';
