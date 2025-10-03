{{ config(materialized='view') }}

with raw as (
  select * from {{ source('movies_raw', 'RAW_GENOME_TAGS') }}
)

select
  "TAGID"::int      as tag_id,
  "TAG"  as tag
from raw
