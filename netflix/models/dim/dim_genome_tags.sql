{{ config(materialized='table') }}

with src as (
  select * from {{ ref('src_genome_tags') }}
)

select
  "TAG_ID"::int                    as tag_id,
  "TAG"                           as tag
from src
