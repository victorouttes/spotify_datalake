{{config(
    alias='popularity_by_artist',
    table_type='iceberg'
)}}

select
	artist_name,
	avg(song_popularity) as popularity_avg
from {{ ref('silver_spotify_recommendations') }}
group by artist_name
order by 2 desc