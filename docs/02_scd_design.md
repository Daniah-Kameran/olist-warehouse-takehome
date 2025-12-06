# 02 – SCD Design (Customer Dimension)

## Why SCD on Customer?
Customer geography (ZIP/city/state) can change over time.  
To support “as-was” vs “as-is” reporting, `dim_customer` is modeled as **Slowly Changing Dimension Type 2**.
This allows:
- analysing spending before/after a move,
- correct historical attribution of sales to the customer’s location at purchase time.

---

## Table structure
`dim_customer` keeps multiple versions per customer:

- `effective_date` = start of validity for that version
- `expiry_date` = end of validity (`9999-12-31` for current)
- `current_flag` = TRUE only for the latest row. 

---

## Daily snapshot approach
Each refresh day we create a “today snapshot”:

1. **Create staging snapshot**
```sql
CREATE OR REPLACE TEMP VIEW stg_customer_snapshot AS
SELECT
  c.customer_id,
  c.customer_unique_id,
  g.geography_sk,
  c.customer_city,
  c.customer_state,
  current_date() AS snapshot_date
FROM olist_customers_dataset c
LEFT JOIN dim_geography g
  ON c.customer_zip_code_prefix = g.zip_code_prefix;
