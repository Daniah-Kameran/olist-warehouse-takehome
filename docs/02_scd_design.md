# 02 — SCD Design (dim_customer Type-2)

## Why SCD2 for customers?
Customer location attributes (ZIP, city, state, geography_sk) matter for reporting and segmentation.  
Even if customer geography does not change frequently, supporting **as-of historical analysis** is required.  

SCD Type-2 preserves history:
- When attributes change → expire current row.
- Insert a new version with a new surrogate key.
- Keep exactly one “current” row using `current_flag = TRUE`. 

## Trigger attributes
Changes in any of the following create a new version:
- customer_city
- customer_state
- geography_sk (derived from ZIP prefix)


## dim_customer base build
Since Olist sources contain only the latest version, the initial dim build creates a single current row per customer with an open history window.

```sql
CREATE OR REPLACE TABLE workspace.default.dim_customer (
  customer_sk        BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id        STRING,
  customer_unique_id STRING,
  geography_sk       BIGINT,
  customer_city      STRING,
  customer_state     STRING,
  effective_date     DATE,
  expiry_date        DATE,
  current_flag       BOOLEAN
);

INSERT INTO workspace.default.dim_customer (
  customer_id,
  customer_unique_id,
  geography_sk,
  customer_city,
  customer_state,
  effective_date,
  expiry_date,
  current_flag
)
SELECT
    c.customer_id,
    c.customer_unique_id,
    g.geography_sk,
    c.customer_city,
    c.customer_state,
    DATE '1900-01-01' AS effective_date,
    DATE '9999-12-31' AS expiry_date,
    TRUE              AS current_flag
FROM workspace.default.olist_customers_dataset c
LEFT JOIN workspace.default.dim_geography g
  ON CAST(c.customer_zip_code_prefix AS INT) = g.zip_code_prefix;
