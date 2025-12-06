


---

## `docs/03_denormalised_layer.md`

```md
# 03 — Denormalised Reporting Layer

## Purpose
To make analytics simple for BI / ad-hoc usage, a wide “sales baseline” view is created.  
It keeps:
- **Fact grain** = order item
- **Historical correctness** using customer SCD “as-of” joins
- **Lifecycle attributes** (first_order_date, new_customer_flag)



## Baseline sales view
```sql
CREATE OR REPLACE VIEW workspace.default.vw_sales_order_item_baseline AS
WITH base AS (
  SELECT
      f.order_id,
      f.order_item_id,
      f.customer_id,
      f.customer_sk,
      f.product_sk,
      f.seller_sk,
      f.geography_sk,
      f.date_sk,
      f.payment_sk,
      f.price,
      f.freight_value,
      (f.price + f.freight_value) AS item_total,

      d.calendar_date AS order_date,
      d.year          AS order_year,
      d.month         AS order_month
  FROM workspace.default.fact_order_item f
  JOIN workspace.default.dim_date d
    ON f.date_sk = d.date_sk
),

cust_first_order AS (
  SELECT
      c.customer_unique_id,
      MIN(b.order_date) AS first_order_date
  FROM base b
  JOIN workspace.default.dim_customer c
    ON b.customer_id = c.customer_id
   AND b.order_date BETWEEN c.effective_date AND c.expiry_date
  GROUP BY c.customer_unique_id
)

SELECT
    b.order_id,
    b.order_item_id,

    -- Date
    b.order_date,
    b.order_year,
    b.order_month,

    -- Measures
    b.price,
    b.freight_value,
    b.item_total,

    -- Product
    p.product_id,
    p.product_category_name AS product_category_pt,
    p.product_name_english  AS product_category_en,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,

    -- Seller
    s.seller_id,
    s.seller_city,
    s.seller_state,

    -- Customer AS-OF purchase time
    c.customer_unique_id,
    c.customer_city  AS customer_city_asof,
    c.customer_state AS customer_state_asof,
    cg.zip_code_prefix AS customer_zip_asof,
    cg.latitude        AS customer_lat_asof,
    cg.longitude       AS customer_lng_asof,

    -- Payment
    pay.payment_type,
    pay.payment_installments,

    -- Lifecycle
    fco.first_order_date,
    CASE WHEN b.order_date = fco.first_order_date THEN TRUE ELSE FALSE END AS new_customer_flag

FROM base b
JOIN workspace.default.dim_customer c
  ON b.customer_id = c.customer_id
 AND b.order_date BETWEEN c.effective_date AND c.expiry_date
LEFT JOIN workspace.default.dim_geography cg
  ON c.geography_sk = cg.geography_sk
JOIN workspace.default.dim_product p
  ON b.product_sk = p.product_sk
JOIN workspace.default.dim_seller s
  ON b.seller_sk  = s.seller_sk
LEFT JOIN workspace.default.dim_payment pay
  ON b.payment_sk = pay.payment_sk
LEFT JOIN cust_first_order fco
  ON c.customer_unique_id = fco.customer_unique_id;

