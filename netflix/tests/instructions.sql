-----------------------------------------------------------------------
-- 1) ROLE & WAREHOUSE
-----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-----------------------------------------------------------------------
-- 2) DATABASE & SCHEMAS
-----------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS MOVIELENS;
CREATE SCHEMA   IF NOT EXISTS MOVIELENS.RAW;
CREATE SCHEMA   IF NOT EXISTS MOVIELENS.DEV;     -- where dbt writes your models (if desired)

-- Privileges for the transform role
GRANT ALL ON DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;

-----------------------------------------------------------------------
-- 3) (OPTIONAL) SERVICE USER (NO PASSWORD INCLUDED)
-----------------------------------------------------------------------
-- Uncomment / adapt if you need a dedicated service user
-- CREATE USER IF NOT EXISTS DBT_USER
--   LOGIN_NAME        = 'DBT_USER'
--   DEFAULT_ROLE      = TRANSFORM
--   DEFAULT_WAREHOUSE = 'COMPUTE_WH'
--   DEFAULT_NAMESPACE = 'MOVIELENS.RAW'
--   MUST_CHANGE_PASSWORD = FALSE;
-- GRANT ROLE TRANSFORM TO USER DBT_USER;

-----------------------------------------------------------------------
-- 4) SET DEFAULT CONTEXT
-----------------------------------------------------------------------
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MOVIELENS;
USE SCHEMA RAW;

-----------------------------------------------------------------------
-- 5) RAW TABLES (match your CSV headers exactly)
-----------------------------------------------------------------------

-- movies.csv
CREATE OR REPLACE TABLE RAW_MOVIES (
  movieId INTEGER,
  title   STRING,
  genres  STRING
);

-- ratings.csv
CREATE OR REPLACE TABLE RAW_RATINGS (
  userId     INTEGER,
  movieId    INTEGER,
  rating     FLOAT,
  timestamp  BIGINT
);

-- tags.csv
CREATE OR REPLACE TABLE RAW_TAGS (
  userId     INTEGER,
  movieId    INTEGER,
  tag        STRING,
  timestamp  BIGINT
);

-- genome-scores.csv
CREATE OR REPLACE TABLE RAW_GENOME_SCORES (
  movieId    INTEGER,
  tagId      INTEGER,
  relevance  FLOAT
);

-- genome-tags.csv
CREATE OR REPLACE TABLE RAW_GENOME_TAGS (
  tagId  INTEGER,
  tag    STRING
);

-- links.csv
CREATE OR REPLACE TABLE RAW_LINKS (
  movieId  INTEGER,
  imdbId   INTEGER,
  tmdbId   INTEGER
);

-----------------------------------------------------------------------
-- 6) (OPTIONAL) FILE FORMAT FOR LOCAL COPY (if you ever use COPY INTO)
-----------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT CSV_STD
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- NOTE: For Jupyter/Python loading you don’t need stages/COPY INTO.
-- You’ll load with pandas → Snowflake using write_pandas().
-----------------------------------------------------------------------
-- END
-----------------------------------------------------------------------

pip install snowflake-connector-python pandas

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ---- Connection (fill in placeholders; keep secrets in env vars or keychain) ----
conn = snowflake.connector.connect(
    account='<ACCOUNT_IDENTIFIER>',    # e.g. abcd-xy12345
    user='<USER>',
    role='TRANSFORM',
    warehouse='COMPUTE_WH',
    database='MOVIELENS',
    schema='RAW',
    # authenticator='externalbrowser',  # optional, if using SSO
    # password='<PASSWORD>'            # or use Python keyring/env vars
)

cs = conn.cursor()

def load_csv_to_table(csv_path, table_name, dtype=None):
    """
    Loads a local CSV into a Snowflake table using write_pandas.
    Assumes target table already exists (from instructions.sql).
    dtype: optional dict of column dtypes for pandas.read_csv
    """
    df = pd.read_csv(csv_path, dtype=dtype)
    # write_pandas automatically creates a temp stage and does a fast load
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
    print(f"Loaded {nrows} rows into {table_name}: success={success}, chunks={nchunks}")

# ---- Load each dataset ----

# movies.csv
load_csv_to_table(
    csv_path='path/to/movies.csv',
    table_name='RAW_MOVIES',
    dtype={'movieId': 'Int64'}  # optional
)

# ratings.csv
load_csv_to_table(
    csv_path='path/to/ratings.csv',
    table_name='RAW_RATINGS',
    dtype={'userId': 'Int64', 'movieId': 'Int64', 'rating': 'float64', 'timestamp': 'Int64'}
)

# tags.csv
load_csv_to_table(
    csv_path='path/to/tags.csv',
    table_name='RAW_TAGS',
    dtype={'userId': 'Int64', 'movieId': 'Int64', 'tag': 'string', 'timestamp': 'Int64'}
)

# genome-scores.csv
load_csv_to_table(
    csv_path='path/to/genome-scores.csv',
    table_name='RAW_GENOME_SCORES',
    dtype={'movieId': 'Int64', 'tagId': 'Int64', 'relevance': 'float64'}
)

# genome-tags.csv
load_csv_to_table(
    csv_path='path/to/genome-tags.csv',
    table_name='RAW_GENOME_TAGS',
    dtype={'tagId': 'Int64', 'tag': 'string'}
)

# links.csv
load_csv_to_table(
    csv_path='path/to/links.csv',
    table_name='RAW_LINKS',
    dtype={'movieId': 'Int64', 'imdbId': 'Int64', 'tmdbId': 'Int64'}
)

cs.close()
conn.close()
