{% snapshot snap_tags %}
{{
    config(
        target_schema='snapshots',
        unique_key=['user_id', 'movie_id', 'tagged_at'],
        strategy='timestamp',
        updated_at='tagged_at',
        invalidate_hard_deletes=True
    )
}}
SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id', 'movie_id', 'tagged_at']) }} AS surrogate_key,
    user_id,
    movie_id,
    tag,
    tagged_at
FROM {{ ref('src_tags') }}
{% endsnapshot %}
