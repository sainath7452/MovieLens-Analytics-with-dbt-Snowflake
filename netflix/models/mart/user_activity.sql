{{ config(materialized='table', tags=['marts','reporting']) }}

with ratings as (
  select * from {{ ref('dim_ratings') }}
),
tags as (
  select * from {{ ref('dim_tags') }}
),
users as (
  select * from {{ ref('dim_users') }}
)

select
  u.user_id,
  count(distinct ratings.movie_id) as distinct_movies_rated,
  count(ratings.rating)             as total_ratings,
  count(tags.tag)                   as total_tags,
  min(ratings.rated_at)             as first_activity,
  max(ratings.rated_at)             as last_activity
from users u
left join ratings on u.user_id = ratings.user_id
left join tags on u.user_id = tags.user_id
group by u.user_id
