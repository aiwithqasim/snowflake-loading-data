--===================================
-- Loading data using Web Interface
--===================================

-- Creating a testing database
CREATE DATABASE TEST_DB;
USE DATABASE TEST_DB;

-- customer table
CREATE TABLE CUSTOMER_DETAILS (
    first_name STRING,
    last_name STRING,
    address STRING,
    city STRING,
    state STRING
);

-- table should be empty
SELECT * FROM CUSTOMER_DETAILS;

-- Now Load data into CUSTOMER_DETAILS

--===================================
-- Loading data using SnowCLI
--===================================

-- login snowsql
snowsql

-- Create pipe format
CREATE OR REPLACE FILE FORMAT PIPE_FORMAT_CLI
	type = 'CSV'
	field_delimiter = '|'
	skip_header = 1;
	
-- Create a stage table
CREATE OR REPLACE STAGE PIP_CLI_STAGE
	file_format = PIP_FORMAT_CLI;	

-- put data into stage
put
file://C:\Users\QasimHassan\Downloads\snowflake_project\Data\customer_detail.csv
@PIP_CLI_STAGE auto_compress=true;

-- list stage to see how many files are there
list @PIP_CLI_STAGE;

-- Resume warehouse, in case the auto-resume feature is OFF
ALTER WAREHOUSE <name> RESUME;

-- copy data from stage to table
COPY INTO CUSTOMER_DETAILS
	FROM @PIP_CLI_STAGE
	file_format = (format_name = PIP_FORMAT_CLI)
	on_error = 'skip_file';
	
-- We can also give a COPY command with the pattern  if  your stage contains multiple  files
COPY INTO mycsvtable
	FROM @mycsvstage
	file_format = (format_name = PIP_FORMAT_CLI)
	pattern = '*.contain[1-5].csv.gz'
	on_error = 'skip_file';

--===================================
-- Loading data using Cloud Provider
--===================================

-- tesla table
CREATE OR REPLACE TABLE TESLA_STOCKS(
    date DATE,
    open_value DOUBLE,
    high_vlaue DOUBLE,
    low_value DOUBLE,
    close_vlaue DOUBLE,
    adj_close_value DOUBLE,
    volume BIGINT
);

-- should be empty
SELECT * FROM TESLA_STOCKS;

-- external stage creation
CREATE OR REPLACE STAGE BULK_COPY_TESLA_STOCKS
URL = "s3://snowflake-demo-qh/TSLA.csv"
CREDENTIALS = (AWS_KEY_ID='<access_key>', AWS_SECRET_KEY='<secret_key>');

-- list stage
LIST @BULK_COPY_TESLA_STOCKS;

-- copy data from stage to table
COPY INTO TESLA_STOCKS
	FROM @BULK_COPY_TESLA_STOCKS
	file_format = (TYPE = 'CSV', FIELD_DELIMITER=',', SKIP_HEADER=1)
    on_error = 'skip_file';

-- data should be there
SELECT * FROM TESLA_STOCKS;

------------------------
-- Storage Integration
------------------------

-- giving privileges
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO SYSADMIN;
USE ROLE SYSADMIN;

-- storage integration
CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = '<role arn>'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('<bucket-prefix URL>');

-- giving privileges
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION S3_INTEGRATION TO ROLE SYSADMIN;
USE ROLE SYSADMIN;

-- valdating integration
DESC INTEGRATION S3_INTEGRATION;

-- creating stage
CREATE OR REPLACE STAGE S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS
  STORAGE_INTEGRATION = S3_INTEGRATION
  URL = '<bucket-prefix URL>/TSLA.csv'
  FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER=',', SKIP_HEADER=1);

-- validating integration
LIST @S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS;

-- Need to give the snowflake ARN & ID

-- Making sure the table is empty
TRUNCATE TABLE TESLA_STOCKS;
SELECT * FROM TESLA_STOCKS;

-- Copy data using integration
COPY INTO TESLA_STOCKS FROM @S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS;

-- data should be there
SELECT * FROM TESLA_STOCKS;

--==============================
-- Loading data using Snow Pipe
--===============================

-- 1. Stage the data
-- 2. Test the copy command
-- 3. Create pipe
-- 4. Configure cloud event / call snow pipe rest API

-- truncating data again
TRUNCATE TABLE TESLA_STOCKS;

-- dropping previously create integration & stage
DROP STORAGE INTEGRATION S3_INTEGRATION;
DROP STAGE S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS;

-- HELP: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
-- Step 1: Configure access permissions (policy) for the S3 bucket
-- Step 2: Create the IAM Role in AWS and attach above policy you created.

-- Step 3: Create a Cloud Storage Integration in Snowflake
CREATE OR REPLACE STORAGE INTEGRATION S3_TESLA_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<role-arn>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-demo-qh/input/');

-- Step 4: Retrieve the AWS IAM User for your Snowflake Account
DESC INTEGRATION S3_TESLA_INTEGRATION;

-- Step 5: Grant the IAM User Permissions to Access Bucket Objects
-- STORAGE_AWS_ROLE_ARN 
-- STORAGE_AWS_EXTERNAL_ID

-- Step 6: Create file format for external stage
CREATE OR REPLACE FILE FORMAT S3_TESLA_STAGE_FORMAT
    TYPE= 'CSV'
    FIELD_DELIMITER=','
    SKIP_HEADER=1;

-- Step 6: Create an external stage using file format createbavove
CREATE STAGE S3_TESLA_STAGE
  STORAGE_INTEGRATION = S3_TESLA_INTEGRATION
  URL = 's3://snowflake-demo-qh/input/'
  FILE_FORMAT = S3_TESLA_STAGE_FORMAT;

-- Step 7: Create a COPY Into Command
-- HELP: https://docs.snowflake.com/en/user-guide/data-load-s3-copy

COPY INTO TESLA_STOCKS FROM @S3_TESLA_STAGE;

-- validating & dropping again for pip
SELECT * FROM TESLA_STOCKS;
TRUNCATE TABLE TESLA_STOCKS;

--  Creating Pipe 
CREATE OR REPLACE PIPE S3_TESLA_PIPE AUTO_INGEST=TRUE AS
COPY INTO TESLA_STOCKS FROM @S3_TESLA_STAGE;

-- Configure cloud event / call snow pipe rest API (S3_TESLA_EVENT_NOTICTATION)
SHOW PIPES;

-- Data should be there auotmatically
SELECT * FROM TESLA_STOCKS;

-- DROPPING PIPE
DROP PIPE S3_TESLA_PIPE;


----------------
-- TIME TRAVEL
----------------
SELECT * FROM TESLA_STOCKS order by DATE desc;

-- dropping & getting back the table (time travel)
DROP TABLE TESLA_STOCKS;
UNDROP TABLE TESLA_STOCKS;

-- updating values
UPDATE TESLA_STOCKS SET OPEN_VALUE=200 WHERE DATE = '2022-08-01';

-- getting data beofre last upodate query
SELECT * FROM TESLA_STOCKS BEFORE (statement => '01b73059-0002-f530-0000-fc77000146da') ORDER BY DATE DESC;
