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

-- create pipe format
CREATE OR REPLACE FILE FORMAT PIPE_FORMAT_CLI
	type = 'CSV'
	field_delimiter = '|'
	skip_header = 1;
	
-- create stage table
CREATE OR REPLACE STAGE PIP_CLI_STAGE
	file_format = PIP_FORMAT_CLI;	

-- put data into stage
put
file://<url to>\customer_detail.csv
@PIP_CLI_STAGE auto_compress=true;

-- list stage to see how many fiels are there
list @PIP_CLI_STAGE;

-- resume warehouse, in case autoresume feature is OFF
ALTER WAREHOUSE <name> RESUME;

-- copy data from stage to table
COPY INTO CUSTOMER_DETAILS
	FROM @PIP_CLI_STAGE
	file_format = (format_name = PIP_FORMAT_CLI)
	on_error = 'skip_file';
	
-- we can also give COPY command with pattern  if  your stage contain multiple  files
COPY INTO mycsvtable
	FROM @mycsvstage
	file_format = (format_name = PIP_FORMAT_CLI)
	pattern = '.contain[1-5].csv.gz'
	on_error = 'skip_file';

--===================================
-- Loading data using Cloud Provider
--===================================

-- tesla table
CREATE OR REPLACE TABLE  TESLA_STOCKS(
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

-- making sure table is empty
TRUNCATE TABLE TESLA_STOCKS;
SELECT * FROM TESLA_STOCKS;

-- copy data using integration
COPY INTO TESLA_STOCKS FROM @S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS;

-- data should be there
SELECT * FROM TESLA_STOCKS;