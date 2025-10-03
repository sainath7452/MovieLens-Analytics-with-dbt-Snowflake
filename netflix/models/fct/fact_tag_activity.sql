{{ config(materialized='table') }}

with src as (
  select * from {{ ref('src_tags') }}
)

select
  user_id::int                      as user_id,
  movie_id::int                     as movie_id,
  tag                                as tag,
  cast(tagged_at as timestamp_ntz)   as tagged_at,
  date_trunc('day', cast(tagged_at as timestamp_ntz)) as tagged_day,
  1                                  as tag_event_count
from src
