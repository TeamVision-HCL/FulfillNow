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

