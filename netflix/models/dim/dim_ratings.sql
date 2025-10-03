{{ config(materialized='table') }}

with src as (
  select * from {{ ref('src_ratings') }}
)

select
  USER_ID::int                     as user_id,
  MOVIE_ID::int                    as movie_id,
  RATING::float                   as rating,
  rated_at                        as rated_at

from src
