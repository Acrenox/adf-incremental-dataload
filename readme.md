# 01 - Watermark-Based Incremental Load

## Overview
Tracks the last loaded timestamp in a control table. Each pipeline 
run fetches only records newer than the stored watermark.

## Pipeline Activities
| # | Activity | Type | Purpose |
|---|---|---|---|
| 1 | LKP_GetOldWatermark | Lookup | Reads last loaded time from control table |
| 2 | LKP_GetNewWatermark | Lookup | Gets MAX(modified_date) from source |
| 3 | CPY_IncrementalLoad | Copy | Loads only new records between old and new watermark |
| 4 | SP_UpdateWatermark | Stored Procedure | Updates watermark to new value after success |

## Pipeline Flow
LKP_GetOldWatermark → LKP_GetNewWatermark → CPY_IncrementalLoad → SP_UpdateWatermark

## Key Query
SELECT * FROM dbo.sales_orders
WHERE modified_date > '@{activity('LKP_GetOldWatermark').output.firstRow.last_loaded_time}'
AND modified_date <= '@{activity('LKP_GetNewWatermark').output.firstRow.new_watermark}'

## Watermark Update Expression
@formatDateTime(activity('LKP_GetNewWatermark').output.firstRow.new_watermark, 'yyyy-MM-dd HH:mm:ss')

## When to Use
- Source table has a reliable modified_date or updated_at column
- You only need to track inserts and updates (not deletes)
- Simple, low complexity incremental pattern

## Limitations
- Cannot detect DELETE operations
- Late arriving data with backdated timestamps can be missed
