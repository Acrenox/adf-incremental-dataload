03 - Change Data Capture (CDC)
Overview
Reads row-level changes (INSERT, UPDATE, DELETE) directly from the SQL Server transaction log using CDC. Tracks changes using LSN (Log Sequence Number) — a binary position in the transaction log — instead of a timestamp or ID column.

Pipeline Flow
LKP_GetLastLSN → LKP_GetCurrentLSN → CPY_CDCLoad → SP_UpdateCDCWatermark
Pipeline Activities
#	Activity	Type	Purpose
1	LKP_GetLastLSN	Lookup	Reads last LSN position from CDC watermark table
2	LKP_GetCurrentLSN	Lookup	Gets current MAX LSN from the database live
3	CPY_CDCLoad	Copy	Loads all changes (INSERT/UPDATE/DELETE) between last and current LSN
4	SP_UpdateCDCWatermark	Stored Procedure	Updates CDC watermark to current LSN after success
Database Objects
Object	Type	Purpose
control.cdc_watermark	Table	Stores last loaded LSN per table
control.cdc_lsn_stage	Table	Staging table to convert binary LSN to string for ADF compatibility
dbo.sales_orders	Table	Source table (reused from Project 1)
dbo.sales_orders_cdc_sink	Table	Destination/sink table with cdc_operation column
control.usp_UpdateCDCWatermark	Stored Procedure	Updates CDC watermark after each successful run
Key Queries
LKP_GetLastLSN:

SELECT CONVERT(VARCHAR(100), last_lsn, 1) AS last_lsn
FROM control.cdc_watermark
WHERE table_name = 'sales_orders'
LKP_GetCurrentLSN:

# 03 - Change Data Capture (CDC)

## Overview
Reads row-level changes (INSERT, UPDATE, DELETE) directly from the **SQL Server transaction log** using CDC. Tracks changes using **LSN (Log Sequence Number)** — a binary position in the transaction log — instead of a timestamp or ID column.

---

## Pipeline Flow
```
LKP_GetLastLSN → LKP_GetCurrentLSN → CPY_CDCLoad → SP_UpdateCDCWatermark
```

---

## Pipeline Activities

| # | Activity | Type | Purpose |
|---|---|---|---|
| 1 | LKP_GetLastLSN | Lookup | Reads last LSN position from CDC watermark table |
| 2 | LKP_GetCurrentLSN | Lookup | Gets current MAX LSN from the database live |
| 3 | CPY_CDCLoad | Copy | Loads all changes (INSERT/UPDATE/DELETE) between last and current LSN |
| 4 | SP_UpdateCDCWatermark | Stored Procedure | Updates CDC watermark to current LSN after success |

---

## Database Objects

| Object | Type | Purpose |
|---|---|---|
| `control.cdc_watermark` | Table | Stores last loaded LSN per table |
| `control.cdc_lsn_stage` | Table | Staging table to convert binary LSN to string for ADF compatibility |
| `dbo.sales_orders` | Table | Source table (reused from Project 1) |
| `dbo.sales_orders_cdc_sink` | Table | Destination/sink table with `cdc_operation` column |
| `control.usp_UpdateCDCWatermark` | Stored Procedure | Updates CDC watermark after each successful run |

---

## Key Queries

**LKP_GetLastLSN:**
```sql
SELECT CONVERT(VARCHAR(100), last_lsn, 1) AS last_lsn
FROM control.cdc_watermark
WHERE table_name = 'sales_orders'
```

**LKP_GetCurrentLSN:**
```sql
INSERT INTO control.cdc_lsn_stage (lsn_value)
SELECT CONVERT(VARCHAR(100), sys.fn_cdc_get_max_lsn(), 1);

SELECT TOP 1 lsn_value AS current_lsn
FROM control.cdc_lsn_stage
ORDER BY created_at DESC;
CPY_CDCLoad Source Query:

```

**CPY_CDCLoad Source Query:**
```sql
SELECT 
    order_id,
    customer_name,
    product,
    quantity,
    unit_price,
    total_amount,
    order_status,
    modified_date,
    CASE __$operation
        WHEN 1 THEN 'DELETE'
        WHEN 2 THEN 'INSERT'
        WHEN 4 THEN 'UPDATE'
    END AS cdc_operation
FROM cdc.fn_cdc_get_all_changes_dbo_sales_orders(
    CONVERT(BINARY(10), '@{activity('LKP_GetLastLSN').output.firstRow.last_lsn}', 1),
    CONVERT(BINARY(10), '@{activity('LKP_GetCurrentLSN').output.firstRow.current_lsn}', 1),
    'all'
)
SP_UpdateCDCWatermark Parameters:

Name	Type	Value
table_name	String	sales_orders
last_lsn	String	@{activity('LKP_GetCurrentLSN').output.firstRow.current_lsn}
How It Works
Run	Operation	CDC Captures
Run 1	INSERT new record	cdc_operation = INSERT
Run 2	UPDATE existing record	cdc_operation = UPDATE
Run 3	DELETE existing record	cdc_operation = DELETE
What Makes CDC Unique vs Project 1 & 2
Feature	Project 1 (Watermark)	Project 2 (Delta Copy)	Project 3 (CDC)
Tracks INSERT	✅	✅	✅
Tracks UPDATE	✅	❌	✅
Tracks DELETE	❌	❌	✅
Needs timestamp column	✅	❌	❌
Needs auto-increment ID	❌	✅	❌
Database tier required	Any	Any	Standard S3 or higher
Important Notes
CDC must be enabled at both database level and table level
Requires Standard S3 tier or higher on Azure SQL (not supported on Basic)
CDC data is retained for a limited time (default 3 days) — run pipeline frequently
Binary LSN values must be converted to VARCHAR for ADF compatibility
CDC only captures changes after it is enabled — existing data is not captured
When to Use
You need to capture DELETE operations
Source table has no reliable timestamp or ID column
High accuracy change tracking is required
Azure SQL Standard tier or higher is available
Limitations
Not supported on Free, Basic, or Standard S0/S1/S2 tiers
CDC retention window expires (default 3 days)
Higher complexity to set up compared to watermark or delta copy
Azure SQL and SQL Server only
```

**SP_UpdateCDCWatermark Parameters:**
| Name | Type | Value |
|---|---|---|
| `table_name` | String | `sales_orders` |
| `last_lsn` | String | `@{activity('LKP_GetCurrentLSN').output.firstRow.current_lsn}` |

---

## How It Works

| Run | Operation | CDC Captures |
|---|---|---|
| Run 1 | INSERT new record | `cdc_operation = INSERT` |
| Run 2 | UPDATE existing record | `cdc_operation = UPDATE` |
| Run 3 | DELETE existing record | `cdc_operation = DELETE` |

---

## What Makes CDC Unique vs Project 1 & 2

| Feature | Project 1 (Watermark) | Project 2 (Delta Copy) | Project 3 (CDC) |
|---|---|---|---|
| Tracks INSERT | ✅ | ✅ | ✅ |
| Tracks UPDATE | ✅ | ❌ | ✅ |
| Tracks DELETE | ❌ | ❌ | ✅ |
| Needs timestamp column | ✅ | ❌ | ❌ |
| Needs auto-increment ID | ❌ | ✅ | ❌ |
| Database tier required | Any | Any | Standard S3 or higher |

---

## Important Notes
- CDC must be enabled at both **database level** and **table level**
- Requires **Standard S3 tier or higher** on Azure SQL (not supported on Basic)
- CDC data is retained for a limited time (default 3 days) — run pipeline frequently
- Binary LSN values must be converted to VARCHAR for ADF compatibility
- CDC only captures changes **after** it is enabled — existing data is not captured

---

## When to Use
- You need to capture DELETE operations
- Source table has no reliable timestamp or ID column
- High accuracy change tracking is required
- Azure SQL Standard tier or higher is available

## Limitations
- Not supported on Free, Basic, or Standard S0/S1/S2 tiers
- CDC retention window expires (default 3 days)
- Higher complexity to set up compared to watermark or delta copy
- Azure SQL and SQL Server only
