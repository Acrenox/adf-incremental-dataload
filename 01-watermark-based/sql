-- ================================================
-- PROJECT 1: WATERMARK-BASED INCREMENTAL LOAD
-- ================================================

-- NOTE: Run CREATE SCHEMA separately first if not already created
-- CREATE SCHEMA control;

-- Step 1: Create watermark control table
CREATE TABLE control.watermark_table (
    table_name       VARCHAR(100) PRIMARY KEY,
    last_loaded_time DATETIME
);

-- Step 2: Insert initial watermark (1900-01-01 loads everything on first run)
INSERT INTO control.watermark_table VALUES ('sales_orders', '1900-01-01');

-- Step 3: Create source table
CREATE TABLE dbo.sales_orders (
    order_id      INT IDENTITY(1,1) PRIMARY KEY,
    customer_name VARCHAR(100),
    product       VARCHAR(100),
    quantity      INT,
    unit_price    FLOAT,
    total_amount  FLOAT,
    order_status  VARCHAR(50),
    modified_date DATETIME DEFAULT GETDATE()
);

-- Step 4: Insert sample data
INSERT INTO dbo.sales_orders (customer_name, product, quantity, unit_price, total_amount, order_status, modified_date) VALUES
('Alice Johnson',  'Laptop',       1,  999.99,  999.99,  'Delivered', '2026-01-01 09:00:00'),
('Bob Smith',      'Mouse',        2,   29.99,   59.98,  'Delivered', '2026-01-05 10:30:00'),
('Carol White',    'Keyboard',     1,   79.99,   79.99,  'Shipped',   '2026-01-10 11:00:00'),
('David Lee',      'Monitor',      2,  299.99,  599.98,  'Delivered', '2026-01-15 14:00:00'),
('Eva Martinez',   'Headphones',   1,  149.99,  149.99,  'Delivered', '2026-01-20 16:00:00'),
('Frank Brown',    'Webcam',       3,   59.99,  179.97,  'Cancelled', '2026-01-25 09:00:00'),
('Grace Kim',      'USB Hub',      2,   39.99,   79.98,  'Delivered', '2026-02-01 10:00:00'),
('Henry Wilson',   'Laptop Stand', 1,   49.99,   49.99,  'Shipped',   '2026-02-05 13:00:00'),
('Isla Thomas',    'SSD 1TB',      2,  109.99,  219.98,  'Delivered', '2026-02-10 15:00:00'),
('Jack Davis',     'Mousepad',     4,   19.99,   79.96,  'Delivered', '2026-02-15 08:00:00');

-- Step 5: Create sink table
CREATE TABLE dbo.sales_orders_sink (
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
-- Step 6: Create stored procedure (run separately)
-- ================================================
CREATE PROCEDURE control.usp_UpdateWatermark
    @table_name       VARCHAR(100),
    @last_loaded_time DATETIME
AS
BEGIN
    UPDATE control.watermark_table
    SET last_loaded_time = @last_loaded_time
    WHERE table_name = @table_name;
END;

-- ================================================
-- INCREMENTAL TEST: Insert new records to test
-- ================================================
-- INSERT INTO dbo.sales_orders (customer_name, product, quantity, unit_price, total_amount, order_status, modified_date) VALUES
-- ('Karen Scott',  'Charger',     2,  19.99,  39.98, 'Delivered', '2026-03-01 10:00:00'),
-- ('Liam Turner',  'Smart Watch', 1, 199.99, 199.99, 'Shipped',   '2026-03-02 11:00:00');

-- ================================================
-- RESET: Use these to reset and retest
-- ================================================
-- TRUNCATE TABLE dbo.sales_orders_sink;
-- UPDATE control.watermark_table SET last_loaded_time = '1900-01-01' WHERE table_name = 'sales_orders';
