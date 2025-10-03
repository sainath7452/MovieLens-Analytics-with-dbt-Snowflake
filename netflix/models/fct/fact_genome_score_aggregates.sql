{{ config(materialized='table') }}

with src as (
  select * from {{ ref('src_genome_scores') }}
),

tagged as (
  select
    movieid::int as movie_id,
    tagid::int   as tag_id,
    relevance::float as relevance
  from src
),

ranked as (
  select
    movie_id,
    tag_id,
    relevance,
    row_number() over (partition by movie_id order by relevance desc) as rn
  from tagged
)

select
  movie_id,
  tag_id,
  relevance
from ranked
where rn <= 10   -- top 10 tags by relevance per movie
order by movie_id, relevance desc
