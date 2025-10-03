{{ config(materialized='view') }}

with raw as (
  select * from {{ source('movies_raw', 'RAW_RATINGS') }}
)

select
  USERID::int   as user_id,
  MOVIEID::int  as movie_id,
  RATING::float as rating,
  -- convert unix seconds -> timestamp (NTZ = no timezone)
  dateadd(second, cast(TIMESTAMP as bigint), '1970-01-01'::timestamp_ntz) as rated_at
from raw
