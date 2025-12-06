-- ==================SCD IMPLEMENTION====================
-- ======================================================
-- SCD + MERGE 
-- 1) Daily snapshot view (today’s version of customers)
CREATE OR REPLACE TEMP VIEW stg_customer_snapshot AS
SELECT
    c.customer_id,
    c.customer_unique_id,
    g.geography_sk,
    c.customer_city,
    c.customer_state,
    current_date() AS snapshot_date
FROM workspace.default.olist_customers_dataset c
LEFT JOIN workspace.default.dim_geography g
  ON CAST(c.customer_zip_code_prefix AS INT) = g.zip_code_prefix;

-- 2) Build MERGE source with forced insert rows for changes
CREATE OR REPLACE TEMP VIEW scd_source AS
SELECT
    s.customer_id,
    s.customer_unique_id,
    s.geography_sk,
    s.customer_city,
    s.customer_state,
    s.snapshot_date,
    FALSE AS force_insert
FROM stg_customer_snapshot s

UNION ALL

SELECT
    s.customer_id,
    s.customer_unique_id,
    s.geography_sk,
    s.customer_city,
    s.customer_state,
    s.snapshot_date,
    TRUE AS force_insert
FROM stg_customer_snapshot s
JOIN workspace.default.dim_customer t
  ON t.customer_id = s.customer_id
 AND t.current_flag = TRUE
WHERE
     t.customer_city  <> s.customer_city
  OR t.customer_state <> s.customer_state
  OR t.geography_sk   <> s.geography_sk;

-- 3) Single MERGE: expire old + insert new
MERGE INTO workspace.default.dim_customer AS target
USING scd_source AS source
ON target.customer_id = source.customer_id
AND target.current_flag = TRUE
AND source.force_insert = FALSE

WHEN MATCHED
  AND (
       target.customer_city  <> source.customer_city
    OR target.customer_state <> source.customer_state
    OR target.geography_sk   <> source.geography_sk
  )
THEN UPDATE SET
    target.expiry_date  = date_sub(source.snapshot_date, 1),
    target.current_flag = FALSE

WHEN NOT MATCHED THEN
  INSERT (
      customer_id,
      customer_unique_id,
      geography_sk,
      customer_city,
      customer_state,
      effective_date,
      expiry_date,
      current_flag
  )
  VALUES (
      source.customer_id,
      source.customer_unique_id,
      source.geography_sk,
      source.customer_city,
      source.customer_state,
      source.snapshot_date,
      DATE '9999-12-31',
      TRUE
  );
-- ==================Validation SCD================
-- ================================================
--Simulate customer attribute changes to validate the SCD Type 2 implementation
--step 1 : create a copy of Dim customer just for testing without impacting the result 
CREATE OR REPLACE TABLE workspace.default.dim_customer_copy AS
SELECT * FROM workspace.default.dim_customer;

-- step 2 : picking random 5 customers to do the simulation on
SELECT customer_id
FROM workspace.default.olist_customers_dataset
LIMIT 5;
-- in this stiumlation the foolowing customer_id was picked 
--  06b8999e2fba1a1fbc88172c00ba8bc7
--  18955e83d337fd6b2def6b18a428ac77
--  4e7b3e00288586ebd08712fdd0374a03
--  b2b6027bc5c5109e529d4dc6358b12c3
--  4f2d8ab171c80ec8364f7c12e35b23ad

-- step 3 :build fake tomorrow snapshot with fake changes for the customer_ids we picked in pervouis step 
CREATE OR REPLACE TEMP VIEW stg_customer_snapshot AS
SELECT
    c.customer_id,
    c.customer_unique_id,
    g.geography_sk,

    -- Simulated city change
    CASE 
      WHEN c.customer_id IN ('06b8999e2fba1a1fbc88172c00ba8bc7','18955e83d337fd6b2def6b18a428ac77','4e7b3e00288586ebd08712fdd0374a03','b2b6027bc5c5109e529d4dc6358b12c3','4f2d8ab171c80ec8364f7c12e35b23ad')
      THEN 'simulated_city'
      ELSE c.customer_city
    END AS customer_city,

    -- Simulated state change
    CASE 
      WHEN c.customer_id IN ('06b8999e2fba1a1fbc88172c00ba8bc7','18955e83d337fd6b2def6b18a428ac77','4e7b3e00288586ebd08712fdd0374a03','b2b6027bc5c5109e529d4dc6358b12c3','4f2d8ab171c80ec8364f7c12e35b23ad')
      THEN 'SC'
      ELSE c.customer_state
    END AS customer_state,

    date_add(current_date(), 1) AS snapshot_date   -- pretend snapshot is tomorrow
FROM workspace.default.olist_customers_dataset c
LEFT JOIN workspace.default.dim_geography g
  ON CAST(c.customer_zip_code_prefix AS INT) = g.zip_code_prefix;
  
  
 -- step 4 :Building scd_source (same logic, but will apply to copy)
CREATE OR REPLACE TEMP VIEW scd_source AS
SELECT
    s.customer_id,
    s.customer_unique_id,
    s.geography_sk,
    s.customer_city,
    s.customer_state,
    s.snapshot_date,
    FALSE AS force_insert
FROM stg_customer_snapshot s

UNION ALL

SELECT
    s.customer_id,
    s.customer_unique_id,
    s.geography_sk,
    s.customer_city,
    s.customer_state,
    s.snapshot_date,
    TRUE AS force_insert
FROM stg_customer_snapshot s
JOIN workspace.default.dim_customer_copy t
  ON t.customer_id = s.customer_id
 AND t.current_flag = TRUE
WHERE
     t.customer_city  <> s.customer_city
  OR t.customer_state <> s.customer_state
  OR t.geography_sk   <> s.geography_sk;


-- step 5 : Run the Type-2 SCD MERGE into the copy
MERGE INTO workspace.default.dim_customer_copy AS target
USING scd_source AS source
ON target.customer_id = source.customer_id
AND target.current_flag = TRUE
AND source.force_insert = FALSE

WHEN MATCHED
  AND (
       target.customer_city  <> source.customer_city
    OR target.customer_state <> source.customer_state
    OR target.geography_sk   <> source.geography_sk
  )
THEN UPDATE SET
    target.expiry_date  = date_sub(source.snapshot_date, 1),
    target.current_flag = FALSE

WHEN NOT MATCHED THEN
  INSERT (
      customer_id,
      customer_unique_id,
      geography_sk,
      customer_city,
      customer_state,
      effective_date,
      expiry_date,
      current_flag
  )
  VALUES (
      source.customer_id,
      source.customer_unique_id,
      source.geography_sk,
      source.customer_city,
      source.customer_state,
      source.snapshot_date,
      DATE '9999-12-31',
      TRUE
  );
-- step 6 : validation showing customer change history 
SELECT
  customer_id,
  customer_city,
  customer_state,
  effective_date,
  expiry_date,
  current_flag
FROM workspace.default.dim_customer_copy
WHERE customer_id IN ('06b8999e2fba1a1fbc88172c00ba8bc7','18955e83d337fd6b2def6b18a428ac77','4e7b3e00288586ebd08712fdd0374a03','b2b6027bc5c5109e529d4dc6358b12c3','4f2d8ab171c80ec8364f7c12e35b23ad')
ORDER BY customer_id, effective_date;


