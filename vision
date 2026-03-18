-- =========================================================
-- 1. CREATE WAREHOUSE (COMPUTE ENGINE)
-- =========================================================
CREATE OR REPLACE WAREHOUSE bronze_wh
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

USE WAREHOUSE bronze_wh;

-- =========================================================
-- 2. DATABASE & SCHEMA SETUP
-- =========================================================
CREATE OR REPLACE DATABASE fulfillnow_db;
USE DATABASE fulfillnow_db;

-- Bronze = Raw data
CREATE OR REPLACE SCHEMA bronze;

-- Silver = Cleaned data
CREATE OR REPLACE SCHEMA silver;

-- Gold = Final analytics
CREATE OR REPLACE SCHEMA gold;

-- =========================================================
-- 3. FILE FORMAT (FIX DATE ISSUE DD-MM-YYYY)
-- =========================================================

use schema bronze;
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
DATE_FORMAT = 'DD-MM-YYYY';

-- =========================================================
-- 4. INTERNAL STAGE (NO S3 / NO INTEGRATION)
-- =========================================================
CREATE OR REPLACE STAGE bronze_stage
FILE_FORMAT = csv_format;

-- Upload files manually:
-- PUT file://orders.csv @bronze_stage;

-- =========================================================
-- 5. BRONZE LAYER (RAW TABLES)
-- =========================================================
USE SCHEMA bronze;

CREATE OR REPLACE TABLE raw_customers (
customer_id INT,
customer_name STRING,
email STRING,
phone NUMBER,
city STRING,
state STRING,
segment STRING,
registration_date String
);

CREATE OR REPLACE TABLE raw_employees (
employee_id INT,
name STRING,
role STRING,
shift_start string,
shift_end string,
hub_id INT,
join_date string,
status STRING
);

CREATE OR REPLACE TABLE raw_orders (
order_id INT,
customer_id INT,
order_date DATE,
promised_ship_date DATE,
order_value NUMBER(10,2),
item_count INT,
order_channel STRING,
priority_flag INT,
status STRING
);

CREATE OR REPLACE TABLE raw_inventory (
inventory_id INT,
sku_id INT,
sku_name STRING,
category STRING,
on_hand_qty INT,
reserved_qty INT,
reorder_level INT,
last_restock_date DATE,
warehouse_id INT
);

CREATE OR REPLACE TABLE raw_events (
event_id INT,
order_id INT,
employee_id INT,
event_type STRING,
event_time TIMESTAMP,
remarks STRING
);

list @bronze_stage;
-- =========================================================
-- 6. LOAD DATA (COPY INTO)
-- =========================================================
COPY INTO raw_customers FROM @bronze_stage/customers_master.csv;
COPY INTO raw_employees FROM @bronze_stage/employees_master.csv;
COPY INTO raw_orders FROM @bronze_stage/orders_master.csv;
COPY INTO raw_inventory FROM @bronze_stage/inventory_master.csv;
COPY INTO raw_events FROM @bronze_stage/events_master.csv;

-- =========================================================
-- 7. SILVER LAYER (DATA CLEANING & VALIDATION)
-- =========================================================
USE SCHEMA silver;

-- -------------------------
-- Customers (Deduplication)
-- -------------------------
CREATE OR REPLACE TABLE customers_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY registration_date DESC) rn
    FROM fulfillnow_db.bronze.raw_customers
)
WHERE rn = 1;
select * from customers_clean;
-- -------------------------
-- Employees
-- -------------------------
CREATE OR REPLACE TABLE employees_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY join_date DESC) rn
    FROM fulfillnow_db.bronze.raw_employees
)
WHERE rn = 1;


select * from employees_clean;
-- -------------------------
-- Orders (Business Rules)
-- -------------------------
CREATE OR REPLACE TABLE orders_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) rn
    FROM fulfillnow_db.bronze.raw_orders
)
WHERE rn = 1
AND item_count >= 1
AND order_value >= 0
AND promised_ship_date >= order_date;


select * from orders_clean;

-- -------------------------
-- Inventory
-- -------------------------
CREATE OR REPLACE TABLE inventory_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY warehouse_id, sku_id ORDER BY last_restock_date DESC) rn
    FROM fulfillnow_db.bronze.raw_inventory
)
WHERE rn = 1
AND on_hand_qty >= 0
AND reserved_qty >= 0;

-- -------------------------
-- Events
-- -------------------------
CREATE OR REPLACE TABLE events_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time DESC) rn
    FROM fulfillnow_db.bronze.raw_events
)
WHERE rn = 1
AND event_type IN ('pick','pack','qc','ship','rework','qc_failed');

-- =========================================================
-- 8. DATA VALIDATION (REFERENTIAL INTEGRITY)
-- =========================================================

-- Orders must have valid customers
CREATE OR REPLACE TABLE orders_valid AS
SELECT o.*
FROM orders_clean o
JOIN customers_clean c
ON o.customer_id = c.customer_id;

-- Events must have valid orders & employees
CREATE OR REPLACE TABLE events_valid AS
SELECT e.*
FROM events_clean e
JOIN orders_valid o ON e.order_id = o.order_id
JOIN employees_clean emp ON e.employee_id = emp.employee_id;

-- Time validation
CREATE OR REPLACE TABLE events_final AS
SELECT e.*
FROM events_valid e
JOIN orders_valid o
ON e.order_id = o.order_id
WHERE e.event_time >= o.order_date;

-- =========================================================
-- 9. ANOMALY DETECTION (STORED PROCEDURE)
-- =========================================================
CREATE OR REPLACE TABLE anomaly_log (
id STRING,
anomaly_type STRING,
description STRING,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE PROCEDURE detect_anomalies()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- Rule 1: SLA Breach
INSERT INTO anomaly_log
SELECT order_id, 'SLA_BREACH', 'Late shipment', CURRENT_TIMESTAMP
FROM orders_clean
WHERE promised_ship_date < CURRENT_DATE();

-- Rule 2: Inventory mismatch
INSERT INTO anomaly_log
SELECT inventory_id, 'INVENTORY_MISMATCH', 'Reserved > stock', CURRENT_TIMESTAMP
FROM inventory_clean
WHERE reserved_qty > on_hand_qty;

-- Rule 3: Event sequence error
INSERT INTO anomaly_log
SELECT order_id, 'SEQUENCE_ERROR', 'Invalid process flow', CURRENT_TIMESTAMP
FROM (
    SELECT order_id,
           LISTAGG(event_type,'->') 
           WITHIN GROUP (ORDER BY event_time) seq
    FROM events_clean
    GROUP BY order_id
)
WHERE seq NOT LIKE '%pick%pack%qc%ship%';

RETURN 'ANOMALY DETECTION COMPLETED';

END;
$$;

-- Run manually
CALL detect_anomalies();

-- =========================================================
-- 10. STREAMS (CHANGE DATA CAPTURE)
-- =========================================================
CREATE OR REPLACE STREAM orders_stream ON TABLE orders_clean;
CREATE OR REPLACE STREAM events_stream ON TABLE events_clean;

-- =========================================================
-- 11. TASK (AUTOMATION)
-- =========================================================
CREATE OR REPLACE TASK anomaly_task
WAREHOUSE = bronze_wh
SCHEDULE = '10 MINUTE'
AS
CALL detect_anomalies();

ALTER TASK anomaly_task RESUME;

-- =========================================================
-- 12. GOLD LAYER (EXCEPTION LOGGING)
-- =========================================================
USE SCHEMA gold;

CREATE OR REPLACE TABLE exception_log (
table_name STRING,
record_id STRING,
error_type STRING,
error_message STRING,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Missing customer
INSERT INTO exception_log
SELECT 'ORDERS', order_id, 'MISSING_CUSTOMER', 'Customer not found', CURRENT_TIMESTAMP
FROM silver.orders_clean o
LEFT JOIN silver.customers_clean c
ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

select * from exception_log;

-- Timeline error
INSERT INTO exception_log
SELECT 'ORDERS', order_id, 'TIMELINE_ERROR', 'Invalid date', CURRENT_TIMESTAMP
FROM silver.orders_clean
WHERE promised_ship_date < order_date;

-- =========================================================
-- 13. DATA MASKING (GOLD LEVEL)
-- =========================================================
CREATE OR REPLACE MASKING POLICY email_mask
AS (val STRING)
RETURNS STRING ->
CASE
WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
ELSE 'masked@email.com'
END;

ALTER TABLE silver.customers_clean
MODIFY COLUMN email
SET MASKING POLICY email_mask;

-- =========================================================
-- 14. ROW LEVEL SECURITY (RLS)
-- =========================================================
CREATE OR REPLACE TABLE employee_role_map(
role_name STRING,
hub_id INT
);

CREATE OR REPLACE ROW ACCESS POLICY employee_rls
AS (hub INT)
RETURNS BOOLEAN ->
EXISTS (
SELECT 1 FROM employee_role_map
WHERE role_name = CURRENT_ROLE()
AND hub_id = hub
);

ALTER TABLE silver.employees_clean
ADD ROW ACCESS POLICY employee_rls ON (hub_id);

-- =========================================================
-- 15. DATA LINEAGE TRACKING
-- =========================================================
CREATE OR REPLACE TABLE data_lineage(
table_name STRING,
load_time TIMESTAMP,
records_loaded INT
);

INSERT INTO data_lineage
SELECT 'orders_clean', CURRENT_TIMESTAMP, COUNT(*)
FROM silver.orders_clean;

select * from data_lineage;

-- =========================================
-- CUSTOMER DIM (SCD-2)
-- =========================================
CREATE OR REPLACE TABLE gold.dim_customers (
customer_sk NUMBER AUTOINCREMENT,
customer_id INT,
customer_name STRING,
email STRING,
phone NUMBER,
segment STRING,
city STRING,
state STRING,
start_date DATE,
end_date DATE,
current_flag STRING
);

-- =========================================
-- EMPLOYEE DIM (SCD-2)
-- =========================================
CREATE OR REPLACE TABLE gold.dim_employees (
employee_sk NUMBER AUTOINCREMENT,
employee_id INT,
name STRING,
role STRING,
shift_start TIMESTAMP,
shift_end TIMESTAMP,
hub_id INT,
start_date DATE,
end_date DATE,
current_flag STRING
);


-- Customers initial load
INSERT INTO gold.dim_customers
SELECT 
SEQ8(), customer_id, customer_name, email, phone,
segment, city, state,
CURRENT_DATE(), NULL, 'Y'
FROM silver.customers_clean;

-- Employees initial load
INSERT INTO gold.dim_employees
SELECT 
SEQ8(),
employee_id,
name,
role,
TO_TIMESTAMP(shift_start,'DD-MM-YYYY'),
TO_TIMESTAMP(shift_end,'DD-MM-YYYY'),
hub_id,
CURRENT_DATE(),
NULL,
'Y'
FROM silver.employees_clean;



MERGE INTO gold.dim_customers tgt
USING silver.customers_clean src
ON tgt.customer_id = src.customer_id
AND tgt.current_flag = 'Y'

-- Step 1: If data changed → expire old record
WHEN MATCHED AND (
tgt.segment != src.segment OR
tgt.city != src.city OR
tgt.state != src.state OR
tgt.email != src.email OR
tgt.phone != src.phone
)
THEN UPDATE SET 
tgt.current_flag = 'N',
tgt.end_date = CURRENT_DATE()

-- Step 2: Insert new record (new OR changed)
WHEN NOT MATCHED THEN
INSERT (
customer_id, customer_name, email, phone,
segment, city, state,
start_date, end_date, current_flag
)
VALUES (
src.customer_id, src.customer_name, src.email, src.phone,
src.segment, src.city, src.state,
CURRENT_DATE(), NULL, 'Y'
);


MERGE INTO gold.dim_employees tgt
USING silver.employees_clean src
ON tgt.employee_id = src.employee_id
AND tgt.current_flag = 'Y'

-- Step 1: expire old record
WHEN MATCHED AND (
tgt.role != src.role OR
tgt.shift_start != TO_TIMESTAMP(src.shift_start,'DD-MM-YYYY') OR
tgt.shift_end != TO_TIMESTAMP(src.shift_end,'DD-MM-YYYY') OR
tgt.hub_id != src.hub_id
)
THEN UPDATE SET
tgt.current_flag = 'N',
tgt.end_date = CURRENT_DATE()

-- Step 2: insert new record
WHEN NOT MATCHED THEN
INSERT (
employee_id, name, role,
shift_start, shift_end, hub_id,
start_date, end_date, current_flag
)
VALUES (
src.employee_id,
src.name,
src.role,
TO_TIMESTAMP(src.shift_start,'DD-MM-YYYY'),
TO_TIMESTAMP(src.shift_end,'DD-MM-YYYY'),
src.hub_id,
CURRENT_DATE(),
NULL,
'Y'
);


CREATE OR REPLACE STREAM customers_stream ON TABLE silver.customers_clean;
CREATE OR REPLACE STREAM employees_stream ON TABLE silver.employees_clean;

MERGE INTO gold.dim_customers tgt
USING (
    SELECT * 
    FROM customers_stream
    WHERE METADATA$ACTION IN ('INSERT','UPDATE')
) src
ON tgt.customer_id = src.customer_id
AND tgt.current_flag = 'Y'

-- =========================================
-- 1. EXPIRE OLD RECORD (IF DATA CHANGED)
-- =========================================
WHEN MATCHED AND (
    NVL(tgt.customer_name,'') != NVL(src.customer_name,'') OR
    NVL(tgt.email,'') != NVL(src.email,'') OR
    NVL(tgt.phone,0) != NVL(src.phone,0) OR
    NVL(tgt.segment,'') != NVL(src.segment,'') OR
    NVL(tgt.city,'') != NVL(src.city,'') OR
    NVL(tgt.state,'') != NVL(src.state,'')
)
THEN UPDATE SET
    tgt.current_flag = 'N',
    tgt.end_date = CURRENT_DATE()

-- =========================================
-- 2. INSERT NEW RECORD (NEW OR CHANGED)
-- =========================================
WHEN NOT MATCHED THEN
INSERT (
    customer_id,
    customer_name,
    email,
    phone,
    segment,
    city,
    state,
    start_date,
    end_date,
    current_flag
)
VALUES (
    src.customer_id,
    src.customer_name,
    src.email,
    src.phone,
    src.segment,
    src.city,
    src.state,
    CURRENT_DATE(),
        NULL,
        'Y'
    );
    
    INSERT INTO gold.dim_customers (
    customer_id, customer_name, email, phone,
    segment, city, state,
    start_date, end_date, current_flag
    )
    SELECT 
    src.customer_id,
    src.customer_name,
    src.email,
    src.phone,
    src.segment,
    src.city,
    src.state,
    CURRENT_DATE(),
    NULL,
    'Y'
    FROM customers_stream src
    JOIN gold.dim_customers tgt
    ON src.customer_id = tgt.customer_id
    WHERE tgt.current_flag = 'N'
    AND tgt.end_date = CURRENT_DATE();
    
    CREATE OR REPLACE TASK scd_customer_task
    WAREHOUSE = bronze_wh
    SCHEDULE = '5 MINUTE'
    AS
    MERGE INTO gold.dim_customers tgt
    USING silver.customers_clean src
    ON tgt.customer_id = src.customer_id
    AND tgt.current_flag='Y'
    WHEN MATCHED AND tgt.segment != src.segment THEN
    UPDATE SET current_flag='N', end_date=CURRENT_DATE()
    WHEN NOT MATCHED THEN
    INSERT VALUES (
    src.customer_id, src.customer_name, src.email,
    src.phone, src.segment, src.city, src.state,
    CURRENT_DATE(), NULL, 'Y'
    );
    
    ALTER TASK scd_customer_task RESUME;




-- =========================================
-- KPI 1: Order Processing SLA Score
-- =========================================
CREATE OR REPLACE VIEW gold.kpi_sla_score AS
SELECT 
    ROUND(
        (COUNT(DISTINCT CASE 
            WHEN e.event_type = 'ship' 
            AND e.event_time <= o.promised_ship_date 
            THEN o.order_id END) * 100.0)
        / COUNT(DISTINCT o.order_id), 2
    ) AS sla_score_percentage
FROM silver.orders_clean o
LEFT JOIN silver.events_clean e
ON o.order_id = e.order_id;


-- =========================================
-- KPI 2: Warehouse Throughput Rate
-- =========================================
CREATE OR REPLACE VIEW gold.kpi_throughput_rate AS
SELECT 
    emp.hub_id,
    DATE_TRUNC('hour', e.event_time) AS event_hour,
    COUNT(*) AS events_per_hour
FROM silver.events_clean e
JOIN silver.employees_clean emp
ON e.employee_id = emp.employee_id
GROUP BY emp.hub_id, event_hour;


-- =========================================
-- KPI 3: Employee Productivity Index
-- =========================================
CREATE OR REPLACE VIEW gold.kpi_employee_productivity AS
SELECT 
    e.employee_id,
    COUNT(ev.event_id) AS total_events,

    DATEDIFF(
        hour, 
        MIN(e.shift_start), 
        MAX(e.shift_end)
    ) AS shift_hours,

    ROUND(
        COUNT(ev.event_id) /
        NULLIF(
            DATEDIFF(hour, MIN(e.shift_start), MAX(e.shift_end)), 0
        ), 2
    ) AS productivity_index

FROM silver.employees_clean e
LEFT JOIN silver.events_clean ev
ON e.employee_id = ev.employee_id
GROUP BY e.employee_id;


-- =========================================
-- KPI 4: Inventory Availability Score
-- =========================================
CREATE OR REPLACE VIEW gold.kpi_inventory_availability AS
SELECT 
    ROUND(
        (COUNT(DISTINCT CASE 
            WHEN e.event_type = 'ship' THEN o.order_id 
        END) * 100.0)
        / COUNT(DISTINCT o.order_id), 2
    ) AS inventory_availability_score
FROM silver.orders_clean o
LEFT JOIN silver.events_clean e
ON o.order_id = e.order_id;


-- =========================================
-- KPI 5: Order Anomaly Rate
-- =========================================
CREATE OR REPLACE VIEW gold.kpi_anomaly_rate AS
SELECT 
    ROUND(
        (COUNT(DISTINCT CASE 
            WHEN a.id IS NOT NULL THEN o.order_id 
        END) * 100.0)
        / COUNT(DISTINCT o.order_id), 2
    ) AS anomaly_rate_percentage
FROM silver.orders_clean o
LEFT JOIN silver.anomaly_log a
ON o.order_id = a.id;


-- =========================================
-- TEST ALL KPIs
-- =========================================
SELECT * FROM gold.kpi_sla_score;
SELECT * FROM gold.kpi_throughput_rate LIMIT 10;
SELECT * FROM gold.kpi_employee_productivity LIMIT 10;
SELECT * FROM gold.kpi_inventory_availability;
SELECT * FROM gold.kpi_anomaly_rate;




-- =========================================
-- RUN KPI QUERIES
-- =========================================
SELECT * FROM gold.kpi_sla_score;
SELECT * FROM gold.kpi_throughput_rate;
SELECT * FROM gold.kpi_employee_productivity;
SELECT * FROM gold.kpi_inventory_availability;
SELECT * FROM gold.kpi_anomaly_rate;

select * from gold.kpi_sla_score;

CREATE OR REPLACE VIEW gold.kpi_employee_productivity AS
SELECT 
    e.employee_id,
    COUNT(ev.event_id) AS total_events,

    DATEDIFF(
        hour, 
        MIN(TO_TIMESTAMP(e.shift_start)), 
        MAX(TO_TIMESTAMP(e.shift_end))
    ) AS shift_hours,

    ROUND(
        COUNT(ev.event_id) /
        NULLIF(
            DATEDIFF(
                hour, 
                MIN(TO_TIMESTAMP(e.shift_start)), 
                MAX(TO_TIMESTAMP(e.shift_end))
            ), 0
        ), 2
    ) AS productivity_index

FROM silver.employees_clean e
LEFT JOIN silver.events_clean ev
ON e.employee_id = ev.employee_id
GROUP BY e.employee_id;

select * from kpi_employee_productivity;

CREATE OR REPLACE VIEW gold.kpi_inventory_availability AS
SELECT 
    ROUND(
        (COUNT(CASE 
            WHEN on_hand_qty >= reserved_qty THEN 1 
        END) * 100.0) / COUNT(*), 2
    ) AS inventory_availability_score
FROM silver.inventory_clean;

select * from kpi_employee_productivity;

SELECT 
o.order_id,
MIN(CASE WHEN e.event_type='ship' THEN e.event_time END) AS ship_time,
o.promised_ship_date
FROM silver.orders_clean o
LEFT JOIN silver.events_clean e
ON o.order_id = e.order_id
GROUP BY o.order_id, o.promised_ship_date;



--- =========================================
-- VIEW 1: Ship Time per Order
-- =========================================
-- Purpose:
-- Get ONE ship event per order (avoid duplicates)

CREATE OR REPLACE VIEW gold.vw_ship_time_per_order AS
SELECT 
    o.order_id,

    -- earliest ship event
    MIN(CASE 
        WHEN e.event_type = 'ship' 
        THEN e.event_time 
    END) AS ship_time,

    o.promised_ship_date

FROM silver.orders_clean o
LEFT JOIN silver.events_clean e
ON o.order_id = e.order_id

GROUP BY o.order_id, o.promised_ship_date;


-- =========================================
-- VIEW 2: SLA Status per Order
-- =========================================
-- Purpose:
-- Classify orders as ON TIME / LATE / NOT SHIPPED

CREATE OR REPLACE VIEW gold.vw_sla_status AS
SELECT 
    order_id,
    ship_time,
    promised_ship_date,

    CASE 
        WHEN ship_time IS NULL THEN 'NOT SHIPPED'
        WHEN ship_time <= promised_ship_date THEN 'ON TIME'
        ELSE 'LATE'
    END AS sla_status

FROM gold.vw_ship_time_per_order;


-- =========================================
-- VIEW 3: Event Sequence per Order
-- =========================================
-- Purpose:
-- Track process flow (pick → pack → qc → ship)

CREATE OR REPLACE VIEW gold.vw_event_sequence AS
SELECT 
    order_id,

    LISTAGG(event_type, ' -> ') 
    WITHIN GROUP (ORDER BY event_time) AS event_sequence

FROM silver.events_clean
GROUP BY order_id;


-- =========================================
-- VIEW 4: Event Count per Order
-- =========================================
-- Purpose:
-- Identify missing or extra events

CREATE OR REPLACE VIEW gold.vw_event_count AS
SELECT 
    order_id,
    COUNT(*) AS total_events
FROM silver.events_clean
GROUP BY order_id;


-- =========================================
-- VIEW 5: Orders Without Shipment
-- =========================================
-- Purpose:
-- Find orders that never reached 'ship'

CREATE OR REPLACE VIEW gold.vw_missing_shipments AS
SELECT DISTINCT o.order_id
FROM silver.orders_clean o
LEFT JOIN silver.events_clean e
ON o.order_id = e.order_id
AND e.event_type = 'ship'
WHERE e.order_id IS NULL;


-- =========================================
-- VIEW 6: SLA Breakdown
-- =========================================
-- Purpose:
-- Total vs on-time orders

CREATE OR REPLACE VIEW gold.vw_sla_breakdown AS
SELECT 
    COUNT(*) AS total_orders,

    COUNT(CASE 
        WHEN ship_time IS NOT NULL 
        AND ship_time <= promised_ship_date 
        THEN 1 
    END) AS on_time_orders

FROM gold.vw_ship_time_per_order;


-- =========================================
-- TEST ALL VIEWS
-- =========================================
SELECT * FROM gold.vw_ship_time_per_order;
SELECT * FROM gold.vw_sla_status;
SELECT * FROM gold.vw_event_sequence;
SELECT * FROM gold.vw_event_count;
SELECT * FROM gold.vw_missing_shipments;
SELECT * FROM gold.vw_sla_breakdown;
