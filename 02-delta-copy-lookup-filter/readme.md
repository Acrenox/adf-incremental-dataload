# 02 - Delta Copy (Lookup + Filter)

## Overview
Tracks the last loaded record using an **auto-increment ID** instead of a timestamp. Uses two Lookup activities to capture a safe bounded range — old `MAX(id)` and new `MAX(id)` — ensuring no records are missed or duplicated during pipeline runs.

---

## Pipeline Flow
```
LKP_GetOldWatermark → LKP_GetNewWatermark → CPY_DeltaLoad → SP_UpdateDeltaWatermark
```

---

## Pipeline Activities

| # | Activity | Type | Purpose |
|---|---|---|---|
| 1 | LKP_GetOldWatermark | Lookup | Reads last loaded `order_id` from delta watermark table |
| 2 | LKP_GetNewWatermark | Lookup | Gets current `MAX(order_id)` from source table |
| 3 | CPY_DeltaLoad | Copy | Loads only records between old and new ID range |
| 4 | SP_UpdateDeltaWatermark | Stored Procedure | Updates delta watermark to new MAX ID after success |

---

## Database Objects

| Object | Type | Purpose |
|---|---|---|
| `control.delta_watermark` | Table | Stores last loaded `order_id` per table |
| `dbo.sales_orders` | Table | Source table (reused from Project 1) |
| `dbo.sales_orders_delta_sink` | Table | Destination/sink table |
| `control.usp_UpdateDeltaWatermark` | Stored Procedure | Updates watermark after each successful run |

---

## Key Queries

**LKP_GetOldWatermark:**
```sql
SELECT last_id FROM control.delta_watermark
WHERE table_name = 'sales_orders'
```

**LKP_GetNewWatermark:**
```sql
SELECT MAX(order_id) AS new_id
FROM dbo.sales_orders
```

**CPY_DeltaLoad Source Query:**
```sql
SELECT * FROM dbo.sales_orders
WHERE order_id > @{activity('LKP_GetOldWatermark').output.firstRow.last_id}
AND order_id <= @{activity('LKP_GetNewWatermark').output.firstRow.new_id}
```

**SP_UpdateDeltaWatermark Parameters:**
| Name | Type | Value |
|---|---|---|
| `table_name` | String | `sales_orders` |
| `last_id` | Int32 | `@{activity('LKP_GetNewWatermark').output.firstRow.new_id}` |

---

## How It Works

| Run | Watermark Before | Records Loaded | Watermark After |
|---|---|---|---|
| Run 1 | `0` | All 12 records | `12` |
| Run 2 | `12` | 2 new records (ID 13, 14) | `14` |

---

## Key Difference from Project 1 (Watermark-Based)

| | Project 1 (Watermark) | Project 2 (Delta Copy) |
|---|---|---|
| Tracks by | `modified_date` timestamp | `order_id` integer |
| Risk | Open-ended, mid-run inserts possible | Bounded range, safe |
| Detects deletes | ❌ | ❌ |
| Detects updates | ✅ (if modified_date changes) | ❌ (ID never changes) |
| Use case | Tables with timestamp columns | Append-only tables with auto-increment ID |

---

## When to Use
- Source table has an auto-increment ID column
- Table is append-only (logs, events, transactions, orders)
- You want a safe bounded range copy with no risk of duplicates
- No timestamp column available on the source

## Limitations
- Cannot detect DELETE operations
- Cannot detect UPDATE operations (ID never changes on update)
- Only works with monotonically increasing ID columns
