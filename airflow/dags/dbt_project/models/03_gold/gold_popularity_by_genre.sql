{{config(
    alias='popularity_by_genre',
    table_type='iceberg'
)}}

select
	genre,
	avg(song_popularity) as popularity_avg
from {{ ref('silver_spotify_recommendations') }}
group by genre
order by 2 desc