{{ config(materialized='table') }}

with from_ratings as (
  select USER_ID::int as user_id from {{ ref('src_ratings') }}
),
from_tags as (
  select USER_ID::int as user_id from {{ ref('src_tags') }}
)

select distinct USER_ID
from (
  select * from from_ratings
  union
  select * from from_tags
)
