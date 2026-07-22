USE ROLE ACCOUNTADMIN;

-- 
USE WAREHOUSE COMPUTE_WH;


CREATE DATABASE IF NOT EXISTS PROJECT2_DB;

USE DATABASE PROJECT2_DB;

CREATE SCHEMA IF NOT EXISTS ICEBERG;

USE SCHEMA ICEBERG;

CREATE OR REPLACE EXTERNAL VOLUME project_two_external_volume
STORAGE_LOCATIONS =
(
    (
        NAME='s3_location'
        STORAGE_PROVIDER='S3'
        STORAGE_BASE_URL='s3://revature-training-389009238812-us-east-2-an/iceberg/'
        STORAGE_AWS_ROLE_ARN='arn:aws:iam::389009238812:role/IcebergPolicy'
    )
)
ALLOW_WRITES = FALSE;



CREATE OR REPLACE CATALOG INTEGRATION project_two_catalog
CATALOG_SOURCE = GLUE
TABLE_FORMAT = ICEBERG
CATALOG_NAMESPACE = 'iceberg_catalog_db'
GLUE_AWS_ROLE_ARN = 'arn:aws:iam::389009238812:role/SnowflakeGlue'
GLUE_CATALOG_ID = '389009238812'
GLUE_REGION = 'us-east-2'
ENABLED = TRUE;


CREATE OR REPLACE ICEBERG TABLE customers
    EXTERNAL_VOLUME = 'project_two_external_volume'
    CATALOG = 'project_two_catalog'
    CATALOG_TABLE_NAME = 'customers';

CREATE OR REPLACE ICEBERG TABLE orders
    EXTERNAL_VOLUME = 'project_two_external_volume'
    CATALOG = 'project_two_catalog'
    CATALOG_TABLE_NAME = 'orders';

CREATE OR REPLACE ICEBERG TABLE products
    EXTERNAL_VOLUME = 'project_two_external_volume'
    CATALOG = 'project_two_catalog'
    CATALOG_TABLE_NAME = 'products';

CREATE OR REPLACE ICEBERG TABLE order_details
    EXTERNAL_VOLUME = 'project_two_external_volume'
    CATALOG = 'project_two_catalog'
    CATALOG_TABLE_NAME = 'order_details';

-- Analysis Query 1:  Categories by revenue
SELECT
    category,
    SUM(total_amount) AS total_revenue,
    COUNT(order_id) AS number_of_orders
FROM order_details
GROUP BY category
ORDER BY total_revenue DESC;


-- Analysis Query 2:  Top Spenders
SELECT
    customer_id,
    first_name,
    last_name,
    SUM(total_amount) AS total_spent,
    COUNT(order_id) AS orders_placed
FROM order_details
GROUP BY
    customer_id,
    first_name,
    last_name
ORDER BY total_spent DESC
LIMIT 5;

-- Analysis query 3: Cost to profit ratio
SELECT
    product_name,
    brand,
    SUM(quantity) AS units_sold,
    SUM(total_amount) AS revenue,
    SUM((price - cost) * quantity) AS estimated_profit
FROM order_details
GROUP BY
    product_name,
    brand
ORDER BY estimated_profit DESC;

-- Analysis Query 4:  Sales by month
SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    COUNT(order_id) AS orders,
    SUM(total_amount) AS revenue
FROM orders
GROUP BY order_month
ORDER BY order_month;

-- Analysis Query 5:  Sales by category
SELECT
    category,
    SUM(total_amount) AS total_profit
FROM order_details
GROUP BY category
ORDER BY total_profit DESC;


DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS order_details;