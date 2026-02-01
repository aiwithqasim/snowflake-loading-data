## Objective
Objective of this project is to getting started in an handson way with snowflake and AWS. Following will be the objectives of this project based learnings

- Overview of Snowflake
- Loading data into snowflake using Web UI
- Loading data into Snowflake using Snow CLI
- Loading data int0 snowflake from S3 using Access/Private keys
- Loading data into snowflake from S3 using Storage Integration
- Loading Real-time data into snowflake using Snowpipe
- Visualaizing the loadded data via AWS QuickSIght
- Understanding pricing of Snowflake
- Time Travel in Snowflake
- Performance optimization in Snowflake

![img-snowflake-loading](./img/flow.png)

## Prerequisites
- A Snowflake account with an active warehouse and appropriate roles/privileges.
- Install `snowsql` (Snowflake CLI) if you want to run commands locally.
- If loading from S3: an AWS account, `aws` CLI (optional), S3 bucket and credentials or an IAM role configured for Snowflake.

## Tech Stack
- Languages: SQL
- Services: Snowflake, SnowSQL, Amazon S3 (optional), QuickSight (optional)

## Contents
- `snowflake intro.sql` — example SQL and SnowSQL commands demonstrating table creation, stages, COPY, storage integration, Snowpipe and time travel.
- `data/` — sample CSV files used by examples (e.g., `data/customer_detail.csv`, `data/TSLA.csv`).
- `img/` — illustrative images.

## Prerequisites
- A Snowflake account with an active warehouse and appropriate roles/privileges.
- Install `snowsql` (Snowflake CLI) if you want to run commands locally.
- If loading from S3: an AWS account, `aws` CLI (optional), S3 bucket and credentials or an IAM role configured for Snowflake.

## Getting started with Hands-on

### ❄️ Overview of Snowflake:

Snowflake is a cloud-native data platform designed for scalable storage, compute separation, and elastic SQL processing. For data engineering use cases Snowflake is commonly used to ingest, transform, and serve data for analytics and downstream applications. Key ideas:

- Data is organized in `DATABASE`s and `SCHEMA`s for logical separation.
- `WAREHOUSE`s provide the compute resources to run queries; they can auto-scale, suspend, and resume.
- `ROLE`s and `USER`s control access and privileges; follow least-privilege practices for production.

Below are short definitions and example SQL for common Snowflake concepts used in data engineering.

- Database: a top-level logical container for schemas and objects. Example:

```sql
CREATE DATABASE SNOWFLAKE_LOADING_DB;
USE DATABASE SNOWFLAKE_LOADING_DB;
```

- Schema: a namespace inside a database that groups tables, views and other objects. Example:

```sql
CREATE SCHEMA SNOWFLAKE_LOADING_SCHEMA;
USE SCHEMA SNOWFLAKE_LOADING_SCHEMA;
```

- Role: defines a set of privileges. Assign roles to users and grant privileges to roles. Example:

```sql
CREATE ROLE SNOWFLAKE_LOADING_ROLE;
GRANT USAGE ON DATABASE SNOWFLAKE_LOADING_DB TO ROLE SNOWFLAKE_LOADING_ROLE;
GRANT USAGE ON SCHEMA SNOWFLAKE_LOADING_DB.SNOWFLAKE_LOADING_SCHEMA TO ROLE SNOWFLAKE_LOADING_ROLE;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA SNOWFLAKE_LOADING_DB.SNOWFLAKE_LOADING_SCHEMA TO ROLE SNOWFLAKE_LOADING_ROLE;
-- Grant role to a user (requires ACCOUNTADMIN or SECURITYADMIN)
GRANT ROLE SNOWFLAKE_LOADING_ROLE TO USER alice;
```

- User: an account-level identity. Create users for service accounts or team members. Example:

```sql
CREATE USER ETL_USER
	PASSWORD = 'ChangeMe123!' -- rotate in production
	DEFAULT_ROLE = SNOWFLAKE_LOADING_ROLE
	DEFAULT_WAREHOUSE = COMPUTE_WH;
```

- Warehouse: the compute resource that executes queries. Size and auto-suspend control cost/latency. Example:

```sql
CREATE WAREHOUSE compute_wh
	WAREHOUSE_SIZE = 'XSMALL'
	AUTO_SUSPEND = 300
	AUTO_RESUME = TRUE
	INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE compute_wh;
```

These small examples are safe to run in a development account; for production, replace passwords with secrets management and create more granular roles and resource monitors.

### 🌐 Loading data into snowflake using Web UI

Create the require table for laoding data into that table.

```sql
USE DATABASE SNOWFLAKE_LOADING_DB;
USE SCHEMA SNOWFLAKE_LOADING_SCHEMA;

CREATE TABLE CUSTOMER_DETAILS (
	first_name STRING,
	last_name STRING,
	address STRING,
	city STRING,
	state STRING
);
SELECT * FROM CUSTOMER_DETAILS;
```

- Goto snowflake and from side menu select ingestion icon and then `Add Data`
- Select `Load data into a Table`
- Browse and load the `data/customer_detail.csv` file into CUSTOMER_DETAILS table
- Make sure to select.
	- CSV as file format
	- PIPE as seperator
	- First file as header

### ⌨️ Loading data into Snowflake using Snow CLI

- Make sure to install Snow CLI using [https://sfc-repo.snowflakecomputing.com/snowflake-cli/index.html](https://sfc-repo.snowflakecomputing.com/snowflake-cli/index.html)

- Launch your terminal (Git Bash, PowerShell, or macOS/Linux terminal) and verify installation

```bash
snow --version
```
- Execute the following commands to add a new connection.

```bash
snow connection add
```
When prompted, provide the required details such as: Connection name, Account identifier, Username, Authentication method (password, SSO, key-pair, etc.) and any optional parameters (role, warehouse, database, schema)

- After creating the connection, verify it using

```bash
snow connection test
```

If configured correctly, you should see a success message confirming the connection.

- Create a file format and stage, then `PUT` the file and `COPY INTO` the table. Example commands (run inside `snowsql` or as script lines where appropriate):

```sql
CREATE OR REPLACE FILE FORMAT PIPE_FORMAT_CLI
	TYPE = 'CSV'
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1;

CREATE OR REPLACE STAGE PIPE_CLI_STAGE
	FILE_FORMAT = PIP_FORMAT_CLI;
```

- Then from your shell (or in `snowsql` with a local client path) to copy data from local to stage suing file format we created above:

```bash
PUT file:///workspaces/snowflake-loading-data/data/customer_detail.csv @PIPE_CLI_STAGE AUTO_COMPRESS=TRUE;
```

- To copy teh data from stage to main table need to run the following copy command

```sql
-- copying data froms tage to table
COPY INTO CUSTOMER_DETAILS
FROM @PIPE_CLI_STAGE
FILE_FORMAT=(format_name=PIPE_FORMAT_CLI)
ON_ERROR='skip_file';

-- verify the table loading
SELECT COUNT(*) FROM CUSTOMER_DETAILS;
```

### 🔑 Loading data into snowflake from S3 using Access/Private keys

### 🔗 Loading data into snowflake from S3 using Storage Integration
### ⚡ Loading Real-time data into snowflake using Snowpipe
### 📊 Visualaizing the loadded data via AWS QuickSIght
### 💰 Understanding pricing of Snowflake
### ⏰ Time Travel in Snowflake
### ⚙️ Performance optimization in Snowflake



6) Load from S3 (simple external stage)
- Upload the file to your S3 bucket and create an external stage or use credentials in the `CREATE STAGE` statement. Example (replace placeholders):

```sql
CREATE OR REPLACE TABLE TESLA_STOCKS(
	date DATE,
	open_value DOUBLE,
	high_vlaue DOUBLE,
	low_value DOUBLE,
	close_vlaue DOUBLE,
	adj_close_value DOUBLE,
	volume BIGINT
);

CREATE OR REPLACE STAGE BULK_COPY_TESLA_STOCKS
	URL = 's3://your-bucket/path/TSLA.csv'
	CREDENTIALS = (AWS_KEY_ID='<access_key>', AWS_SECRET_KEY='<secret_key>');

COPY INTO TESLA_STOCKS
	FROM @BULK_COPY_TESLA_STOCKS
	FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1)
	ON_ERROR = 'skip_file';

SELECT COUNT(*) FROM TESLA_STOCKS;
```

7) Use a Storage Integration (recommended for production)
- Create an IAM role and grant access as shown in `snowflake intro.sql`, then create a storage integration in Snowflake and reference it when creating stages. Key commands:

```sql
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO SYSADMIN;
USE ROLE SYSADMIN;

CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
	TYPE = EXTERNAL_STAGE
	STORAGE_PROVIDER = 'S3'
	STORAGE_AWS_ROLE_ARN = '<role arn>'
	ENABLED = TRUE
	STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-prefix/');

GRANT USAGE ON INTEGRATION S3_INTEGRATION TO ROLE SYSADMIN;
DESC INTEGRATION S3_INTEGRATION;

CREATE OR REPLACE STAGE S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS
	STORAGE_INTEGRATION = S3_INTEGRATION
	URL = 's3://your-bucket-prefix/TSLA.csv'
	FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1);

COPY INTO TESLA_STOCKS FROM @S3_INTEGRATEION_BULK_COPY_TESLA_STOCKS;
```

8) Snowpipe (continuous ingestion)
- High level steps from the repo:
	- Stage the data.
	- Test the `COPY` command.
	- Create a `PIPE` with `AUTO_INGEST=TRUE`.
	- Configure cloud notifications (S3 event notifications) or call the Snowpipe REST API.

Example pipe creation:

```sql
CREATE OR REPLACE PIPE S3_TESLA_PIPE AUTO_INGEST=TRUE AS
	COPY INTO TESLA_STOCKS FROM @S3_TESLA_STAGE;
SHOW PIPES;
SELECT * FROM TESLA_STOCKS;
DROP PIPE S3_TESLA_PIPE;
```

9) Time Travel and object recovery (examples from `snowflake intro.sql`)

```sql
SELECT * FROM TESLA_STOCKS ORDER BY DATE DESC;
DROP TABLE TESLA_STOCKS;
UNDROP TABLE TESLA_STOCKS;
UPDATE TESLA_STOCKS SET OPEN_VALUE = 200 WHERE DATE = '2022-08-01';
SELECT * FROM TESLA_STOCKS BEFORE (statement => '<statement-id>') ORDER BY DATE DESC;
```

## Local files referenced
- Sample data files are under `data/` (e.g., `data/customer_detail.csv`, `data/TSLA.csv`).
- The main instruction script is `snowflake intro.sql` — you can run portions of it directly in Snowflake or with `snowsql`.

## Next steps (optional)
- I can prepare a ready-to-run SnowSQL script that uses workspace-local paths, or preview the CSVs in `data/` and run quick checks. Tell me which you'd like.

---