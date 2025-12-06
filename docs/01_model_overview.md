# 01 – Model Overview (Olist Star Schema)

## Goal
Build an analytics-ready dimensional model on top of the Olist e-commerce dataset to answer:
- sales & revenue performance over time and by segment,
- customer behaviour and lifecycle,
- product & assortment performance,
- historical consistency when attributes change. 

The model is implemented in Databricks / Delta Lake.

---

## Source data
Main Olist tables used:
- `olist_customers_dataset`
- `olist_orders_dataset`
- `olist_order_items_dataset`
- `olist_order_payments_dataset`
- `olist_products_dataset`
- `olist_sellers_dataset`
- `olist_geolocation_dataset`
- `product_category_name_translation` (PT→EN mapping)

---

## Dimensional model choice
I used a **Star Schema** (fact + conformed dimensions) because:
- queries are simpler and faster for BI,
- dataset volume is moderate (no need for snowflake complexity),
- easy to extend with new dimensions later. 

---

## Grain and keys

### Fact grain
**`fact_order_item`** has one row per `(order_id, order_item_id)`.

Why:
- each order can contain multiple items,
- item-level grain supports product/seller profitability and flexible rollups,
- preserves the true business process: “selling one product to a customer by a seller”. 

### Surrogate keys
All dimensions use surrogate keys (`*_sk`) and are joined from the fact through foreign keys:
- `customer_sk`
- `product_sk`
- `seller_sk`
- `geography_sk`
- `date_sk`
- `payment_sk`. :contentReference[oaicite:8]{index=8}

Business/natural keys (e.g., `customer_id`, `product_id`) stay in dimensions for traceability.

---

## Dimensions

| Dimension | Description | Key attributes |
|---|---|---|
| `dim_customer` | customer master data; track location changes via SCD2 | `customer_sk`, `customer_id`, `customer_unique_id`, `geography_sk`, `customer_city`, `customer_state`, `effective_date`, `expiry_date`, `current_flag` |
| `dim_product` | product master + category translation | `product_sk`, `product_id`, PT category, EN category, physical attributes |
| `dim_seller` | seller master | `seller_sk`, `seller_id`, seller city/state |
| `dim_geography` | ZIP prefix based geography | `geography_sk`, zip prefix, city/state, lat/lng |
| `dim_date` | calendar roles for ordering & delivery analysis | `date_sk`, date, year, month, etc. |
| `dim_payment` | payment type & installments | `payment_sk`, payment_type, payment_installments | :contentReference[oaicite:9]{index=9}:contentReference[oaicite:10]{index=10}

---

## How the model answers mandatory questions
Because the fact is at order-item grain and dimensions are conformed:
- **Sales over time:** group by `dim_date` fields, sum `price + freight_value`.  
- **Revenue by segment:** group by product category, seller geography, customer geography.  
- **Top product/segment combinations:** multi-dim groupings on the same fact.  
- **New customers:** first purchase date derived from earliest fact row per customer.  
- **Historical correctness:** join fact to SCD2 customer on effective/expiry windows. :contentReference[oaicite:11]{index=11}:contentReference[oaicite:12]{index=12}

---


- Optional ML questions supported by adding derived feature views. :contentReference[oaicite:13]{index=13}
