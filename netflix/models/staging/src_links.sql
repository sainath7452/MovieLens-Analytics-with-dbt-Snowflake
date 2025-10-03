{{ config(materialized='view') }}

with raw as (
  select * from {{ source('movies_raw', 'RAW_LINKS') }}
)

select
  movieid::int                      as movie_id,
  imdbid                            AS imdb_id,
  tmdbid                            as tmdb_id
from raw
