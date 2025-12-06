
-- =========================
-- dim_customer
-- =========================
-- Fix : rebuild dim_customer with a historical effective_date
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
    DATE '1900-01-01'  AS effective_date,   --  covers all historical orders
    DATE '9999-12-31'  AS expiry_date,
    TRUE               AS current_flag
FROM workspace.default.olist_customers_dataset c
LEFT JOIN workspace.default.dim_geography g
  ON CAST(c.customer_zip_code_prefix AS INT) = g.zip_code_prefix;
  
-- =========================
-- dim_product
-- =========================
CREATE OR REPLACE TABLE dim_product AS
SELECT
    monotonically_increasing_id() AS product_sk,   -- monotonically_increasing_idis a built-in Spark/Databricks SQL function Generates a unique 64-bit integer
    p.product_id,
    p.product_category_name,
    t.product_category_name_english AS product_name_english,
    p.product_name_lenght           AS product_name_length,
    p.product_description_lenght    AS product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM olist_products_dataset p
LEFT JOIN product_category_name_translation t
  ON p.product_category_name = t.product_category_name
WHERE p.product_id IS NOT NULL;

-- =========================
-- dim_payment
-- =========================
CREATE OR REPLACE TABLE dim_payment (
  payment_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  payment_type STRING,
  payment_installments INT
);

TRUNCATE TABLE dim_payment; --avoiding duplicate

INSERT INTO dim_payment (payment_type, payment_installments)
SELECT DISTINCT
    payment_type,
    payment_installments
FROM olist_order_payments_dataset;

-- =========================
-- dim_seller
-- =========================
CREATE OR REPLACE TABLE dim_seller (
  seller_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  seller_id STRING,
  geography_sk BIGINT,
  seller_city STRING,
  seller_state STRING
);

TRUNCATE TABLE dim_seller; -- avoid duplication in the second run 

INSERT INTO dim_seller (seller_id, geography_sk, seller_city, seller_state)
SELECT
    s.seller_id,
    g.geography_sk,
    s.seller_city,
    s.seller_state
FROM olist_sellers_dataset s
LEFT JOIN dim_geography g
  ON CAST(s.seller_zip_code_prefix AS INT) = g.zip_code_prefix;
  
-- =========================
-- dim_date
-- ========================= 
  
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY d) AS date_sk,
    d AS calendar_date,
    YEAR(d)  AS year,
    MONTH(d) AS month,
    DAY(d) AS day,
    DAYOFWEEK(d) AS day_of_week,
    WEEKOFYEAR(d) AS week_of_year
FROM (
    SELECT explode(
        sequence(
            MIN(CAST(order_purchase_timestamp AS DATE)),
            MAX(CAST(order_purchase_timestamp AS DATE)),
            interval 1 day
        )
    ) AS d
    FROM olist_orders_dataset
);

-- =========================
-- dim_geography
-- ========================= 
CREATE OR REPLACE TABLE dim_geography (
  geography_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  zip_code_prefix INT,
  city          STRING,
  state         STRING,
  latitude      DOUBLE,
  longitude     DOUBLE
);

TRUNCATE TABLE dim_geography;

INSERT INTO dim_geography (zip_code_prefix, city, state, latitude, longitude)
SELECT
    CAST(geolocation_zip_code_prefix AS INT)  AS zip_code_prefix,
    FIRST(geolocation_city)                  AS city,
    FIRST(geolocation_state)                 AS state,
    AVG(geolocation_lat)                     AS latitude,
    AVG(geolocation_lng)                     AS longitude
FROM olist_geolocation_dataset
GROUP BY geolocation_zip_code_prefix;

-- =========================
-- FACT TABLE 
-- ========================= 

TRUNCATE TABLE fact_order_item;

WITH first_payment AS (
  SELECT
    order_id,
    payment_type,
    payment_installments,
    ROW_NUMBER() OVER (
      PARTITION BY order_id ORDER BY payment_value DESC
    ) AS rn
  FROM olist_order_payments_dataset
),
dpay_dedup AS (
  SELECT payment_type, payment_installments, MIN(payment_sk) AS payment_sk
  FROM dim_payment
  GROUP BY payment_type, payment_installments
)

INSERT INTO fact_order_item (
    order_id,
    order_item_id,
    customer_sk,
    product_sk,
    seller_sk,
    geography_sk,
    date_sk,
    payment_sk,
    price,
    freight_value,
    customer_id          -- adding the new cliumn in last for alret sprak 
)
SELECT
    oi.order_id,
    oi.order_item_id,
    dc.customer_sk,
    dp.product_sk,
    ds.seller_sk,
    dg.geography_sk,
    dd.date_sk,
    dpay.payment_sk,
    oi.price,
    oi.freight_value,
    o.customer_id AS customer_id   -- same here
FROM olist_order_items_dataset oi
JOIN olist_orders_dataset o
  ON oi.order_id = o.order_id
JOIN dim_customer dc
  ON o.customer_id = dc.customer_id AND dc.current_flag = TRUE
JOIN dim_product dp
  ON oi.product_id = dp.product_id
JOIN dim_seller ds
  ON oi.seller_id = ds.seller_id
LEFT JOIN dim_geography dg
  ON dc.geography_sk = dg.geography_sk
JOIN dim_date dd
  ON CAST(o.order_purchase_timestamp AS DATE) = dd.calendar_date
LEFT JOIN first_payment fp
  ON oi.order_id = fp.order_id AND fp.rn = 1
LEFT JOIN dpay_dedup dpay
  ON fp.payment_type = dpay.payment_type
 AND fp.payment_installments = dpay.payment_installments
WHERE oi.order_id IS NOT NULL;