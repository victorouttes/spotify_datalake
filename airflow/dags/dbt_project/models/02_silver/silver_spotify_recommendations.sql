{{config(
    alias='spotify_recommendations',
    table_type='iceberg'
)}}

select
	*,
	'J-Rock' as genre
from {{ ref('bronze_spotify_recommendations_jrock') }}
union all
select
	*,
	'J-Pop' as genre
from {{ ref('bronze_spotify_recommendations_jpop') }}
union all
select
	*,
	'K-Pop' as genre
from {{ ref('bronze_spotify_recommendations_kpop') }}