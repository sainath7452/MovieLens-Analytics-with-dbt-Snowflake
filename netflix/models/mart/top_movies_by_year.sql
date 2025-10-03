{{ config(
    materialized='incremental',
    unique_key='movie_id_year',
    tags=['marts','analytics']
) }}

with ratings as (
  select * from {{ ref('dim_ratings') }}
),
movies as (
  select * from {{ ref('dim_movies') }}
),
ratings_with_year as (
  select
    r.movie_id,
    m.movie_title,
    r.rating,
    date_trunc('year', r.rated_at) as year
  from ratings r
  left join movies m
    on r.movie_id = m.movie_id
)

select
  r.movie_id,
  r.movie_title,
  extract(year from r.year) as year,
  count(*) as num_ratings,
  avg(r.rating) as avg_rating
from ratings_with_year r
group by r.movie_id, r.movie_title, extract(year from r.year)

{% if is_incremental() %}
  -- only insert rows for years we don't have yet
  where (extract(year from r.year), r.movie_id) not in (
    select year, movie_id from {{ this }}
  )
{% endif %}
