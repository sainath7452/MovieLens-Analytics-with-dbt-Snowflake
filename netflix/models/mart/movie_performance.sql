{{ config(materialized='table', tags=['marts','reporting']) }}

with ratings as (
  select * from {{ ref('dim_ratings') }}
),
movies as (
  select * from {{ ref('dim_movies') }}
)

select
  m.movie_id,
  m.movie_title,
  count(r.rating)                as num_ratings,
  avg(r.rating)                  as avg_rating,
  min(r.rated_at)                as first_rating_at,
  max(r.rated_at)                as last_rating_at
from movies m
left join ratings r on m.movie_id = r.movie_id
group by m.movie_id, m.movie_title
