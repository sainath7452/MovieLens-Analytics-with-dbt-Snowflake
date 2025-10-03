{{ config(materialized='table') }}

with ratings as (
    select * from {{ ref('dim_ratings') }}
),
movies as (
    select * from {{ ref('dim_movies') }}
),
users as (
    select * from {{ ref('dim_users') }}
)

select
    r.user_id,
    u.user_id as user_key,
    m.movie_id as movie_key,
    r.rating,
    r.rated_at
from ratings r
join movies m on r.movie_id = m.movie_id
join users u on r.user_id = u.user_id
