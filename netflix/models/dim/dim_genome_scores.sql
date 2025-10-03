{{ config(materialized='table') }}

with src as (
  select * from {{ ref('src_genome_scores') }}
)

select
  "MOVIE_ID"::int                  as movie_id,
  "TAG_ID"::int                    as tag_id,
  "RELEVANCE"::float              as relevance
from src
