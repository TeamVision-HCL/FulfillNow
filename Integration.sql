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

CREATE TABLE raw_customers (
    customer_id STRING PRIMARY KEY,
    customer_name STRING,
    email STRING,
    phone STRING,
    city STRING,
    state STRING,
    segment STRING,
    registration_date STRING
);

CREATE TABLE raw_employees (
    employee_id STRING PRIMARY KEY,
    name STRING,
    role STRING,
    shift_start STRING,
    shift_end STRING,
    hub_id STRING,
    join_date STRING,
    status STRING
);

CREATE TABLE raw_orders (
    order_id STRING PRIMARY KEY,
    customer_id STRING,
    order_date STRING,
    promised_ship_date STRING,
    order_value STRING,
    item_count STRING,
    order_channel STRING,
    priority_flag STRING,
    status STRING,

    FOREIGN KEY (customer_id) REFERENCES raw_customers(customer_id)
);

CREATE TABLE raw_inventory (
    inventory_id STRING PRIMARY KEY,
    sku_id STRING,
    sku_name STRING,
    category STRING,
    on_hand_qty STRING,
    reserved_qty STRING,
    reorder_level STRING,
    last_restock_date STRING,
    warehouse_id STRING
);

CREATE TABLE raw_events (
    event_id STRING PRIMARY KEY,
    order_id STRING,
    employee_id STRING,
    event_type STRING,
    event_time STRING,
    remarks STRING,

    FOREIGN KEY (order_id) REFERENCES raw_orders(order_id),
    FOREIGN KEY (employee_id) REFERENCES raw_employees(employee_id)
);

COPY INTO raw_customers
FROM @bronze_stage/customers/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

COPY INTO raw_employees
FROM @bronze_stage/employees/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

COPY INTO raw_orders
FROM @bronze_stage/Orders/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

COPY INTO raw_inventory
FROM @bronze_stage/inventory/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

COPY INTO raw_events
FROM @bronze_stage/events/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
