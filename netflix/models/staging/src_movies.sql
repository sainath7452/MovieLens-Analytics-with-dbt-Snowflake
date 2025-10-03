{{ config(materialized='view') }}

with raw as (
  select * from {{ source('movies_raw', 'RAW_MOVIES') }}
)

select
  movieid::int                      as movie_id,
  title                              as title,
  genres                             as genres
from raw
