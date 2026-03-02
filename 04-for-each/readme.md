04 - Date-Based Incremental Load (ForEach + GetMetadata)

Overview
Implements a date-driven incremental load using Azure Data Factory ForEach and Lookup activities. 
The pipeline dynamically generates a list of process dates between the last processed date and the current date, 
then iterates day by day to load only the relevant records. A watermark table ensures fault-tolerant and idempotent execution.

This approach is useful when historical backfill is required or when data arrives sporadically on different dates.

Pipeline Flow
LKP_GetLastProcessedDate → LKP_Generate_Process_Dates → ForEach (Process Date)
    ├── GetMetadata
    ├── CPY_SalesOrders_Incremental
    └── SP_UpdateForEachWatermark

Pipeline Activities

#   Activity Name                    Type              Purpose
1   LKP_GetLastProcessedDate         Lookup            Reads last processed date from foreach watermark table
2   LKP_Generate_Process_Dates       Lookup            Generates list of dates from last processed date to current date
3   fe_ProcessDates                  ForEach           Iterates over each generated process date
4   GetMetadata                      Get Metadata      Optional validation of source/sink (row count, existence)
5   CPY_SalesOrders_Incremental      Copy              Loads data for a single process date
6   SP_UpdateForEachWatermark        Stored Procedure  Updates watermark after successful date load

Database Objects

Object                               Type               Purpose
control.foreach_watermark            Table              Stores last processed date per table
dbo.sales_orders                     Table              Source table
dbo.sales_orders_cdc_sink             Table              Destination/sink table
control.usp_UpdateForEachWatermark   Stored Procedure   Updates last processed date after each successful iteration

Key Queries

LKP_GetLastProcessedDate:
SELECT last_processed_date
FROM control.foreach_watermark
WHERE table_name = 'sales_orders';

LKP_Generate_Process_Dates:
WITH DateCTE AS (
    SELECT CAST(last_processed_date AS DATE) AS process_date
    FROM control.foreach_watermark
    WHERE table_name = 'sales_orders'
    UNION ALL
    SELECT DATEADD(DAY, 1, process_date)
    FROM DateCTE
    WHERE process_date < CAST(GETDATE() AS DATE)
)
SELECT process_date
FROM DateCTE
OPTION (MAXRECURSION 0);

CPY_SalesOrders_Incremental Source Query:
SELECT
    order_id,
    customer_name,
    product,
    quantity,
    unit_price,
    total_amount,
    order_status,
    modified_date,
    GETDATE() AS load_timestamp
FROM dbo.sales_orders
WHERE CAST(modified_date AS DATE) = '@{item().process_date}';

SP_UpdateForEachWatermark Parameters:

Name                 Type     Value
table_name           String   sales_orders
last_processed_date  String   @{item().process_date}

How It Works

Run      Watermark Before   Dates Processed                  Records Loaded   Watermark After
Run 1    2024-09-09         2026-03-01 to 2026-03-03          4                 2026-03-03
Run 2    2026-03-03         No new dates                      0                 2026-03-03

Each ForEach iteration processes exactly one date. 
The watermark is updated only after a successful copy, ensuring safe restart and no data loss.

Key Difference from Other Incremental Strategies

Project                     Increment Logic                  Strength
Project 1 – Watermark       Timestamp-based                  Simple, update-aware
Project 2 – Delta Copy      Auto-increment ID                Safe bounded range
Project 3 – CDC             Change Data Capture (LSN)        Tracks inserts/updates/deletes
Project 4 – ForEach Dates   Date slicing with ForEach        Backfill-friendly, controlled loads

When to Use
Historical backfill is required
Data arrives irregularly across dates
You want controlled, date-wise incremental processing
Large date gaps need safe replay
Operational transparency per processing date is needed

Limitations
Not suitable for high-frequency near-real-time ingestion
Multiple iterations may increase pipeline runtime
Requires careful watermark management
Does not detect deletes unless explicitly handled
