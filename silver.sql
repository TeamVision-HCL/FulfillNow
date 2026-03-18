USE ROLE dev_role;   -- or test_role / analyst_role
USE WAREHOUSE bronze_wh;
USE DATABASE fulfillnow_db;
use schema bronze;
show tables;
select * from RAW_EMPLOYEES;
USE SCHEMA silver;

------------------------------------

CREATE OR REPLACE TABLE customers_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY registration_date DESC
           ) AS rn
    FROM fulfillnow_db.bronze.raw_customers
)
WHERE rn = 1
AND customer_id IS NOT NULL;

select * from customers_clean;

SELECT COUNT(*) 
FROM fulfillnow_db.bronze.raw_employees;

CREATE OR REPLACE TABLE employees_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY employee_id
               ORDER BY join_date DESC
           ) rn
    FROM fulfillnow_db.bronze.raw_employees
)
WHERE rn = 1
AND employee_id IS NOT NULL;

select * from fulfillnow_db.silver.employees_clean;

CREATE OR REPLACE TABLE orders_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY order_date DESC
           ) AS rn
    FROM fulfillnow_db.bronze.raw_orders
)
WHERE rn = 1
AND order_id IS NOT NULL
AND item_count >= 1
AND order_value >= 0
AND promised_ship_date >= order_date;

select * from orders_clean;

CREATE OR REPLACE TABLE inventory_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY warehouse_id, sku_id
               ORDER BY last_restock_date DESC
           ) rn
    FROM fulfillnow_db.bronze.raw_inventory
)
WHERE rn = 1
AND on_hand_qty >= 0
AND reserved_qty >= 0;

select * from inventory_clean;

CREATE OR REPLACE TABLE events_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY event_id
               ORDER BY event_time DESC
           ) AS rn
    FROM fulfillnow_db.bronze.raw_events
)
WHERE rn = 1
AND event_type IN ('pick','pack','qc','ship','rework','qc_failed');

select * from events_clean;
----------------------------------------

CREATE OR REPLACE TABLE orders_valid AS
SELECT o.*
FROM orders_clean o
JOIN customers_clean c
ON o.customer_id = c.customer_id;

select * from orders_valid;

CREATE OR REPLACE TABLE events_valid AS
SELECT e.*
FROM events_clean e
JOIN orders_valid o
ON e.order_id = o.order_id
JOIN employees_clean emp
ON e.employee_id = emp.employee_id;

select * from events_valid;

CREATE OR REPLACE TABLE events_final AS
SELECT e.*
FROM events_valid e
JOIN orders_valid o
ON e.order_id = o.order_id
WHERE e.event_time >= o.order_date;

select * from events_final;

-------------------------------------


