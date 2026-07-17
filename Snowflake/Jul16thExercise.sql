

-- 
USE ROLE ACCOUNTADMIN;

-- 
USE WAREHOUSE COMPUTE_WH;


CREATE DATABASE IF NOT EXISTS PRACTICE_DB;

CREATE SCHEMA IF NOT EXISTS SANDBOX;

-- 
USE DATABASE PRACTICE_DB;

-- 
USE SCHEMA SANDBOX;

CREATE OR REPLACE TABLE phone_numbers(
    number_id INT AUTOINCREMENT PRIMARY KEY,
    number_string STRING NOT NULL
);

INSERT INTO phone_numbers (number_string) VALUES ('513-849-0582'),
('(513) 555-1234'),
('513.777.8888'),
('5138499999');

SELECT * FROM phone_numbers;

CREATE OR REPLACE FUNCTION normalize_phone(phoneNum STRING)
RETURNS STRING
AS
$$
    REGEXP_REPLACE(phoneNum, '[^0-9]', '')
$$;
-- Now regex the string to remove -, (, ), '.'.  


SELECT normalize_phone(number_string) FROM phone_numbers;
 


DROP DATABASE PRACTICE_DB;