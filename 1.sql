CREATE OR REPLACE WAREHOUSE bronze_wh
WITH 
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

USE WAREHOUSE bronze_wh;

CREATE DATABASE fulfillnow_db;

CREATE SCHEMA fulfillnow_db.bronze;

USE DATABASE fulfillnow_db;
USE SCHEMA bronze;

CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::079390754540:role/snowflake_s3_rolee'
STORAGE_ALLOWED_LOCATIONS = ('s3://snowflakre-hackathon-data/');

DESC INTEGRATION s3_int;

CREATE OR REPLACE STAGE bronze_stage
URL = 's3://snowflakre-hackathon-data/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

LIST @bronze_stage;

CREATE OR REPLACE TABLE raw_customers (
    customer_id INT,
    customer_name STRING,
    email STRING,
    phone NUMBER,
    city STRING,
    state STRING,
    segment STRING,
    registration_date DATE
);

CREATE OR REPLACE TABLE raw_employees (
    employee_id INT,
    name STRING,
    role STRING,
    shift_start TIMESTAMP,
    shift_end TIMESTAMP,
    hub_id INT,
    join_date DATE,
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


CREATE OR REPLACE PIPE pipe_raw_customers
AUTO_INGEST = TRUE
AS
COPY INTO raw_customers
FROM @bronze_stage/customers/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

CREATE OR REPLACE PIPE pipe_raw_employees
AUTO_INGEST = TRUE
AS
COPY INTO raw_employees
FROM @bronze_stage/employees/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

CREATE OR REPLACE PIPE pipe_raw_orders
AUTO_INGEST = TRUE
AS
COPY INTO raw_orders
FROM @bronze_stage/Orders/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

CREATE OR REPLACE PIPE pipe_raw_inventory
AUTO_INGEST = TRUE
AS
COPY INTO raw_inventory
FROM @bronze_stage/inventory/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

CREATE OR REPLACE PIPE pipe_raw_events
AUTO_INGEST = TRUE
AS
COPY INTO raw_events
FROM @bronze_stage/events/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

SHOW PIPES;

SELECT SYSTEM$PIPE_STATUS('pipe_raw_customers');

LIST @bronze_stage/customers/;
LIST @bronze_stage/employees/;
LIST @bronze_stage/Orders/;
LIST @bronze_stage/inventory/;
LIST @bronze_stage/events/;

create or replace schema silver;

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

--Stored procedure
CREATE TABLE silver.anomaly_log (
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

---------------------------------------------------
-- RULE 1: MIS-PICK PROBABILITY
---------------------------------------------------
INSERT INTO anomaly_log (id, anomaly_type, description, created_at)
SELECT 
    order_id,
    'MIS_PICK',
    'QC failed or repeated rework events',
    CURRENT_TIMESTAMP
FROM (
    SELECT order_id,
           COUNT_IF(event_type = 'qc_failed') qc_fail_count,
           COUNT_IF(event_type = 'rework') rework_count
    FROM events_clean
    GROUP BY order_id
)
WHERE (qc_fail_count > 0 OR rework_count > 1)
AND NOT EXISTS (
    SELECT 1 FROM anomaly_log a
    WHERE a.id = order_id
      AND a.anomaly_type = 'MIS_PICK'
);

---------------------------------------------------
-- RULE 2: SLA BREACH
---------------------------------------------------
INSERT INTO anomaly_log (id, anomaly_type, description, created_at)
SELECT 
    o.order_id,
    'SLA_BREACH',
    'Order not shipped on time',
    CURRENT_TIMESTAMP
FROM orders_clean o
LEFT JOIN events_clean e 
    ON o.order_id = e.order_id 
    AND e.event_type = 'ship'
WHERE (e.event_id IS NULL OR e.event_time > o.promised_ship_date)
AND NOT EXISTS (
    SELECT 1 FROM anomaly_log a
    WHERE a.id = o.order_id
      AND a.anomaly_type = 'SLA_BREACH'
);

---------------------------------------------------
-- RULE 3: LOW PRODUCTIVITY
---------------------------------------------------
INSERT INTO anomaly_log (id, anomaly_type, description, created_at)
SELECT 
    employee_id,
    'LOW_PRODUCTIVITY',
    'Very low events per shift',
    CURRENT_TIMESTAMP
FROM (
    SELECT employee_id, COUNT(*) event_count
    FROM events_clean
    GROUP BY employee_id
)
WHERE event_count < 5
AND NOT EXISTS (
    SELECT 1 FROM anomaly_log a
    WHERE a.id = employee_id
      AND a.anomaly_type = 'LOW_PRODUCTIVITY'
);

---------------------------------------------------
-- RULE 4: SUSPICIOUS ACTIVITY
---------------------------------------------------
INSERT INTO anomaly_log (id, anomaly_type, description, created_at)
SELECT 
    employee_id,
    'SUSPICIOUS_ACTIVITY',
    'Too many events in short time',
    CURRENT_TIMESTAMP
FROM (
    SELECT employee_id,
           COUNT(*) cnt,
           MIN(event_time) min_t,
           MAX(event_time) max_t
    FROM events_clean
    GROUP BY employee_id
)
WHERE DATEDIFF('minute', min_t, max_t) < 5
  AND cnt > 20
AND NOT EXISTS (
    SELECT 1 FROM anomaly_log a
    WHERE a.id = employee_id
      AND a.anomaly_type = 'SUSPICIOUS_ACTIVITY'
);

---------------------------------------------------
-- RULE 5: INVENTORY MISMATCH
---------------------------------------------------
INSERT INTO anomaly_log (id, anomaly_type, description, created_at)
SELECT 
    inventory_id,
    'INVENTORY_MISMATCH',
    'Reserved qty greater than available',
    CURRENT_TIMESTAMP
FROM inventory_clean
WHERE reserved_qty > on_hand_qty
AND NOT EXISTS (
    SELECT 1 FROM anomaly_log a
    WHERE a.id = inventory_id
      AND a.anomaly_type = 'INVENTORY_MISMATCH'
);

---------------------------------------------------
-- RULE 6: EVENT SEQUENCE ERROR
---------------------------------------------------
INSERT INTO anomaly_log (id, anomaly_type, description, created_at)
SELECT 
    order_id,
    'SEQUENCE_ERROR',
    'Invalid event order',
    CURRENT_TIMESTAMP
FROM (
    SELECT order_id,
           LISTAGG(event_type, '->') 
           WITHIN GROUP (ORDER BY event_time) seq
    FROM events_clean
    GROUP BY order_id
)
WHERE seq NOT LIKE '%pick%pack%qc%ship%'
AND NOT EXISTS (
    SELECT 1 FROM anomaly_log a
    WHERE a.id = order_id
      AND a.anomaly_type = 'SEQUENCE_ERROR'
);

---------------------------------------------------

RETURN 'ANOMALY DETECTION COMPLETED';

END;
$$;

CALL detect_anomalies();
SELECT * 
FROM anomaly_log;


--------------gold layer---------------------
USE SCHEMA fulfillnow_db.silver;

CREATE OR REPLACE STREAM orders_stream
ON TABLE orders_clean;

CREATE OR REPLACE STREAM events_stream
ON TABLE events_clean;

CREATE OR REPLACE STREAM inventory_stream
ON TABLE inventory_clean;

CREATE OR REPLACE STREAM customers_stream
ON TABLE customers_clean;

CREATE OR REPLACE STREAM employees_stream
ON TABLE employees_clean;


use SCHEMA fulfillnow_db.gold ;
CREATE OR REPLACE TABLE silver.exception_log (

table_name STRING,
record_id STRING,
error_type STRING,
error_message STRING,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);
CREATE OR REPLACE TABLE silver.exception_log (

table_name STRING,
record_id STRING,
error_type STRING,
error_message STRING,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

INSERT INTO silver.exception_log

SELECT
'ORDERS',
order_id,
'MISSING_CUSTOMER',
'Customer reference missing',
CURRENT_TIMESTAMP

FROM orders_clean o

LEFT JOIN customers_clean c
ON o.customer_id=c.customer_id

WHERE c.customer_id IS NULL;


INSERT INTO silver.exception_log

SELECT
'ORDERS',
order_id,
'MISSING_CUSTOMER',
'Customer reference missing',
CURRENT_TIMESTAMP

FROM orders_clean o

LEFT JOIN customers_clean c
ON o.customer_id=c.customer_id

WHERE c.customer_id IS NULL;


INSERT INTO silver.exception_log

SELECT

'ORDERS',
order_id,
'TIMELINE_ERROR',
'Promised date before order date',
CURRENT_TIMESTAMP

FROM orders_clean

WHERE promised_ship_date < order_date;

INSERT INTO silver.exception_log

SELECT

'EVENTS',
order_id,
'INVALID_SEQUENCE',
'Wrong event order',
CURRENT_TIMESTAMP

FROM (

SELECT
order_id,
LISTAGG(event_type,'->')
WITHIN GROUP (ORDER BY event_time) seq

FROM events_clean
GROUP BY order_id

)

WHERE seq NOT LIKE '%pick%pack%qc%ship%';


INSERT INTO silver.exception_log

SELECT

'INVENTORY',
inventory_id,
'QUANTITY_ERROR',
'Reserved greater than stock',
CURRENT_TIMESTAMP

FROM inventory_clean

WHERE reserved_qty > on_hand_qty;


CREATE OR REPLACE TASK anomaly_task

WAREHOUSE = bronze_wh

SCHEDULE = '10 MINUTE'

AS

CALL detect_anomalies();


CREATE OR REPLACE TASK kpi_task

WAREHOUSE = bronze_wh

AFTER anomaly_task

AS

INSERT INTO gold.kpi_refresh_log
SELECT CURRENT_TIMESTAMP;


ALTER TASK anomaly_task RESUME;

ALTER TASK kpi_task RESUME;


CREATE OR REPLACE MASKING POLICY email_mask

AS (val STRING)

RETURNS STRING ->

CASE

WHEN CURRENT_ROLE()='ACCOUNTADMIN'

THEN val

ELSE 'masked@email.com'

END;


ALTER TABLE customers_clean

MODIFY COLUMN email

SET MASKING POLICY email_mask;

CREATE MASKING POLICY phone_mask

AS (val NUMBER)

RETURNS NUMBER ->

CASE

WHEN CURRENT_ROLE()='ACCOUNTADMIN'

THEN val

ELSE 0000000000

END;


ALTER TABLE customers_clean

MODIFY COLUMN phone

SET MASKING POLICY phone_mask;

CREATE TABLE employee_role_map(

role_name STRING,
hub_id INT

);

CREATE ROW ACCESS POLICY employee_rls

AS (hub INT)

RETURNS BOOLEAN ->

EXISTS(

SELECT 1

FROM employee_role_map

WHERE role_name=CURRENT_ROLE()

AND hub_id=hub

);

ALTER TABLE employees_clean

ADD ROW ACCESS POLICY employee_rls

ON (hub_id);

CREATE TABLE silver.data_lineage(

table_name STRING,
load_time TIMESTAMP,
records_loaded INT

);

INSERT INTO silver.data_lineage

SELECT

'orders_clean',

CURRENT_TIMESTAMP,

COUNT(*)

FROM orders_clean;


