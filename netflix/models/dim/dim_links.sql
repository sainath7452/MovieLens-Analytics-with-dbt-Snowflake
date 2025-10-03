{{ config(materialized='table') }}

with src as (
  select * from {{ ref('src_links') }}
)

select
  movie_id::int         as movie_id,
  nullif(imdb_id, '')   as imdb_id,
  nullif(tmdb_id, '')   as tmdb_id
from src
