{{config(
    alias='spotify_recommendations_jpop',
    table_type='iceberg'
)}}

select
	track.id as song_id,
	track.name as song_name,
	track.popularity as song_popularity,
	track.album.name as album_name,
	track.album.album_type as album_type,
	artist.name as artist_name,
	track.href as spotify_link,
	track.album.images[1].url as album_image
from {{ source('landing', 'spotify_recommend_tracks_jpop') }}
cross join unnest(tracks) as track
cross join unnest(track.album.artists) as artist