-- models/Staging/src_tags.sql
{{ config(materialized='view') }}

with raw as (
  select * from {{ source('movies_raw', 'RAW_TAGS') }}
)

select
  "USERID"::int    as user_id,
  "MOVIEID"::int   as movie_id,
  "TAG"            as tag,
  -- convert unix seconds -> timestamp (NTZ = no timezone)
  dateadd(second, cast("TIMESTAMP" as bigint), '1970-01-01'::timestamp_ntz) as tagged_at
from raw
