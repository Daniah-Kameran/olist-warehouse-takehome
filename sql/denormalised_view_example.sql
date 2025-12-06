-- =======================baseline sales view=======================================
-- =================================================================================
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

-- first purchase per persistent customer (customer_unique_id)
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

    -- Product attributes 
    p.product_id,
    p.product_category_name AS product_category_pt,
    p.product_name_english  AS product_category_en,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,

    -- Seller attributes
    s.seller_id,
    s.seller_city,
    s.seller_state,

    -- Customer attributes AS-OF purchase time (historical correctness)
    c.customer_unique_id,
    c.customer_city  AS customer_city_asof,
    c.customer_state AS customer_state_asof,
    cg.zip_code_prefix AS customer_zip_asof,
    cg.latitude        AS customer_lat_asof,
    cg.longitude       AS customer_lng_asof,

    -- Payment attributes (mix by type / installments)
    pay.payment_type,
    pay.payment_installments,

    -- Lifecycle flags
    fco.first_order_date,
    CASE WHEN b.order_date = fco.first_order_date THEN TRUE ELSE FALSE END AS new_customer_flag

FROM base b

-- AS-OF join to SCD customer
JOIN workspace.default.dim_customer c
  ON b.customer_id = c.customer_id
 AND b.order_date BETWEEN c.effective_date AND c.expiry_date

LEFT JOIN workspace.default.dim_geography cg
  ON c.geography_sk = cg.geography_sk

JOIN workspace.default.dim_product p
  ON b.product_sk = p.product_sk

JOIN workspace.default.dim_seller s
  ON b.seller_sk = s.seller_sk

LEFT JOIN workspace.default.dim_payment pay
  ON b.payment_sk = pay.payment_sk

LEFT JOIN cust_first_order fco
  ON c.customer_unique_id = fco.customer_unique_id;

--==========================Business queries================================
-- =========================================================================
--Daily sales trend
SELECT
  order_date,
  SUM(item_total) AS total_sales,
  COUNT(DISTINCT order_id) AS num_orders,
  COUNT(*) AS num_items
FROM workspace.default.vw_sales_order_item_baseline
GROUP BY order_date
ORDER BY order_date;

-- monthly sales trend 
SELECT
  order_year,
  order_month,
  SUM(item_total) AS total_sales,
  COUNT(DISTINCT order_id) AS num_orders,
  COUNT(*) AS num_items
FROM workspace.default.vw_sales_order_item_baseline
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

--- Revenue breakdown 

-- revenue by product
SELECT
  product_category_en,
  SUM(item_total) AS category_sales,
  COUNT(*) AS items_sold
FROM workspace.default.vw_sales_order_item_baseline
GROUP BY product_category_en
ORDER BY category_sales DESC;

-- revenue by seller 
SELECT
  seller_id,
  seller_city,
  seller_state,
  SUM(item_total) AS seller_sales,
  COUNT(DISTINCT order_id) AS num_orders
FROM workspace.default.vw_sales_order_item_baseline
GROUP BY seller_id, seller_city, seller_state
ORDER BY seller_sales DESC;

--which combination drive most revenue 
SELECT
  product_category_en,
  customer_state_asof,
  SUM(item_total) AS combo_sales,
  COUNT(*) AS items_sold
FROM workspace.default.vw_sales_order_item_baseline
GROUP BY product_category_en, customer_state_asof
ORDER BY combo_sales DESC
LIMIT 20;

-- top seller 
SELECT
  seller_id,
  product_category_en,
  SUM(item_total) AS combo_sales,
  COUNT(*) AS items_sold
FROM workspace.default.vw_sales_order_item_baseline
GROUP BY seller_id, product_category_en
ORDER BY combo_sales DESC
LIMIT 20;

--New customers per month 
SELECT
  order_year,
  order_month,
  COUNT(DISTINCT customer_unique_id) AS new_customers
FROM workspace.default.vw_sales_order_item_baseline
WHERE new_customer_flag = TRUE
GROUP BY order_year, order_month
ORDER BY order_year, order_month;


--Average basket size by customer state
WITH order_totals AS (
  SELECT
    order_id,
    customer_state_asof,
    SUM(item_total) AS order_total,
    COUNT(*) AS num_items
  FROM workspace.default.vw_sales_order_item_baseline
  GROUP BY order_id, customer_state_asof
)
SELECT
  customer_state_asof,
  AVG(order_total) AS avg_order_value,
  AVG(num_items) AS avg_items_per_order,
  COUNT(DISTINCT order_id) AS orders
FROM order_totals
GROUP BY customer_state_asof
ORDER BY avg_order_value DESC;

----
-- Because the view is as-of SCD, we can track behaviour before vs after a customer’s geography change.

-- below example: spend by customer across their SCD versions
SELECT
  customer_unique_id,
  customer_city_asof,
  customer_state_asof,
  MIN(order_date) AS first_order_in_version,
  MAX(order_date) AS last_order_in_version,
  SUM(item_total) AS sales_in_version,
  COUNT(DISTINCT order_id) AS orders_in_version
FROM workspace.default.vw_sales_order_item_baseline
GROUP BY customer_unique_id, customer_city_asof, customer_state_asof
ORDER BY customer_unique_id, first_order_in_version;

-- ======================= Operational & fulfilment insights=======================================
-- =================================================================================
CREATE OR REPLACE VIEW workspace.default.vw_fulfillment_order_item AS
SELECT
    f.order_id,
    f.order_item_id,
    d.calendar_date AS order_date,

    -- Order timestamps
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.order_status,

    -- Duration 
    datediff(o.order_approved_at, o.order_purchase_timestamp)      AS days_to_approve,
    datediff(o.order_delivered_carrier_date, o.order_approved_at) AS days_to_ship,
    datediff(o.order_delivered_customer_date, o.order_purchase_timestamp) AS days_to_deliver,
    datediff(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS days_vs_estimated,


    f.price,
    f.freight_value,
    (f.price + f.freight_value) AS item_total,

    -- Product
    p.product_id,
    p.product_category_name AS product_category_pt,
    p.product_name_english  AS product_category_en,

    -- Seller
    s.seller_id,
    s.seller_city,
    s.seller_state,

    -- Customer geography 
    c.customer_unique_id,
    c.customer_city  AS customer_city_asof,
    c.customer_state AS customer_state_asof

FROM workspace.default.fact_order_item f

JOIN workspace.default.olist_orders_dataset o
  ON f.order_id = o.order_id

JOIN workspace.default.dim_date d
  ON f.date_sk = d.date_sk

JOIN workspace.default.dim_product p
  ON f.product_sk = p.product_sk

JOIN workspace.default.dim_seller s
  ON f.seller_sk = s.seller_sk


JOIN workspace.default.dim_customer c
  ON f.customer_id = c.customer_id
 AND d.calendar_date BETWEEN c.effective_date AND c.expiry_date;
 
 --==========================Business queries================================
-- =========================================================================
-- Average delivery time by customer region
SELECT
  customer_state_asof,
  AVG(days_to_deliver) AS avg_days_to_deliver,
  COUNT(DISTINCT order_id) AS orders
FROM workspace.default.vw_fulfillment_order_item
WHERE order_status = 'delivered'
GROUP BY customer_state_asof
ORDER BY avg_days_to_deliver DESC;

-- Delivery time by product category
SELECT
  product_category_en,
  AVG(days_to_deliver) AS avg_days_to_deliver,
  PERCENTILE_APPROX(days_to_deliver, 0.9) AS p90_days_to_deliver,
  COUNT(DISTINCT order_id) AS orders
FROM workspace.default.vw_fulfillment_order_item
WHERE order_status = 'delivered'
GROUP BY product_category_en
ORDER BY avg_days_to_deliver DESC;

--Seller performance: fastest vs slowest sellers
SELECT
  seller_id,
  seller_state,
  AVG(days_to_deliver) AS avg_days_to_deliver,
  COUNT(DISTINCT order_id) AS orders
FROM workspace.default.vw_fulfillment_order_item
WHERE order_status = 'delivered'
GROUP BY seller_id, seller_state
HAVING orders >= 20
ORDER BY avg_days_to_deliver ASC;

-- late delivery 
SELECT
  customer_state_asof,
  product_category_en,
  COUNT(*) AS items,
  AVG(CASE WHEN days_vs_estimated > 0 THEN 1 ELSE 0 END) AS late_delivery_rate
FROM workspace.default.vw_fulfillment_order_item
WHERE order_status = 'delivered'
GROUP BY customer_state_asof, product_category_en
ORDER BY late_delivery_rate DESC;

