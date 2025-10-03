{{ config(materialized='view') }}

with raw as (
  select * from {{ source('movies_raw', 'RAW_GENOME_SCORES') }}
)

select
  "MOVIEID"::int    as movie_id,
  "TAGID"::int      as tag_id,
  "RELEVANCE"::float as relevance
from raw
