
---

## `docs/04_cdc_approach.md`

```md
# 04 — CDC (Change Data Capture) Approach

## Analyst scope vs engineering scope
As a data analyst, my role is to define:
- **What business attributes matter**
- **Which changes trigger SCD2 vs overwrite**
- **How reporting must stay historically correct**

The physical ingestion, streaming, and scheduling are **data engineering scope**. I outline the logic needed for business correctness below. :contentReference[oaicite:17]{index=17}

## Event structure and ingestion
Expected CDC event schema (JSON):
- `entity_name` (Customer, OrderItem, etc.)
- `event_time` (when change happened)
- `operation` (insert / update / delete)
- `primary_key`
- `payload` (changed columns + new values)



This is engineering-implemented, but analyst-validated because `event_time` must be retained for historical logic. 
- **Customer events** → update `dim_customer` as **SCD Type-2**
- **Product / Seller** → **Type-1 overwrite** unless history becomes a requirement
- **Order / OrderItem / Payment** → MERGE into facts using stable business keys



## CDC → SCD2 behavior (Customer)
Trigger attributes:
- geography_sk
- customer_city
- customer_state  

When a CDC update arrives:
1. Match the current row (`current_flag=TRUE`)
2. If trigger attrs changed → expire current row  
3. Insert new current row with new effective/expiry window  
4. Never delete historical rows  

This guarantees:
- 1 current version per customer
- full history for “as-was” analysis  


## Keeping fact + denormalised layer consistent
- Fact grain stays **(order_id, order_item_id)**.
- CDC merges must not create duplicate grains.
- Late arriving events should be anchored to **event_time**, not arrival time.  
(Implementation detail is engineering scope; requirement is analyst-defined.) 

## Example hourly CDC flow (architecture)
1. Upstream systems emit hourly JSON change events.
2. Auto Loader ingests into `cdc_bronze_events` (partitioned by entity/date).
3. Silver staging per entity is built hourly.
4. Gold layer applies:
   - Customer SCD2 MERGE
   - Fact MERGEs for order/item/payment
5. Data quality checks:
   - exactly one current row per customer
   - no duplicate fact grains



