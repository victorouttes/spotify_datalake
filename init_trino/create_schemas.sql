create schema if not exists minio.landing with (location = 's3a://landing/');
create schema if not exists minio.prod_bronze with (location = 's3a://bronze/');
create schema if not exists minio.prod_silver with (location = 's3a://silver/');
create schema if not exists minio.prod_gold with (location = 's3a://gold/');