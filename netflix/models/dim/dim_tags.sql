{{ config(materialized='table') }}

with src as (
    select * from {{ ref('src_tags') }}
)

select
    user_id::int   as user_id,
    movie_id::int  as movie_id,
    tag            as tag,
    tagged_at      as tagged_at
from src
