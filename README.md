# Olist Data Warehouse Take-Home 

## Overview
This repository contains a star-schema data warehouse built on the Olist dataset (2016–2018) using Databricks SQL.  
It includes raw table loading, dimension and fact creation, Slowly Changing Dimension (SCD) Type-2 for customers, denormalised reporting views, and a CDC design note.

## Repository structure
.
├── README.md  
├── docs/  
│   ├── 01_model_overview.md  
│   ├── 02_scd_design.md  
│   ├── 03_denormalised_layer.md  
│   └── 04_cdc_approach.md  
├── sql/  
│   ├── create_dim_fact_examples.sql  
│   ├── scd_maintenance_examples.sql  
│   └── denormalised_view_example.sql  
├── notebooks/  
│   └── olist_takehome.ipynb   
└── diagrams/  
    └── olist_star_schema.png 

## How to run (Databricks)
1. Upload raw CSVs and create raw tables.
2. Run the notebook **top-to-bottom**. The pipeline is rerunnable:
   - **Type-1 dimensions** use `CREATE OR REPLACE TABLE AS SELECT` (overwrite).
   - **Fact** uses `TRUNCATE + INSERT INTO` to prevent duplicates.
   - **Customer SCD Type-2** uses Delta `MERGE` (safe to rerun).
3. Views produced:
   - `vw_sales_order_item_baseline`: wide reporting view covering mandatory business questions.
   - `vw_fulfillment_order_item`: optional fulfillment/operational insights.

## Notebook execution note (schema qualification)
All SQL in the Databricks notebook assumes the working schema is **`workspace.default`**.  
To make the notebook portable, table references are schema-qualified in the final version:

```sql
FROM workspace.default.olist_order_items_dataset oi
JOIN workspace.default.olist_orders_dataset o
  ON oi.order_id = o.order_id
JOIN workspace.default.dim_customer dc
  ON o.customer_id = dc.customer_id AND dc.current_flag = TRUE
...
