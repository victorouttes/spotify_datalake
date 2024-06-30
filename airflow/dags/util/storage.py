import json
import logging
import os
from urllib.parse import urlparse

import boto3
import pandas as pd
from trino import dbapi


class Storage:
    def __init__(self):
        self.storage_options = {
            'client_kwargs': {
                'endpoint_url': os.environ.get('MINIO_ENDPOINT'),
                'aws_access_key_id': os.environ.get('MINIO_ACCESS_KEY'),
                'aws_secret_access_key': os.environ.get('MINIO_SECRET_KEY'),
                'region_name': 'us-east-1',
                'use_ssl': False
            }
        }
        self.s3_client = boto3.client(
            's3',
            region_name='us-east-1',
            endpoint_url=os.environ.get('MINIO_ENDPOINT'),
            aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY')
        )
        self.trino_host = 'trino-coordinator'
        self.trino_port = 8080
        self.trino_user = 'admin'
        self.trino_catalog = 'minio'
        self.trino_schema = 'landing'

    def __decompose_s3_path(self, s3_path: str) -> tuple:
        parsed_url = urlparse(s3_path)
        bucket = parsed_url.netloc
        key = parsed_url.path.lstrip('/')
        return bucket, key

    def save_dataframe_to_bucket(self, data: pd.DataFrame, path: str):
        data.to_parquet(path=path, storage_options=self.storage_options)
        logging.log(level=logging.INFO, msg='Success!')

    def save_top_songs_recommendation_to_bucket(self, data: dict, path: str):
        logging.log(level=logging.INFO, msg='Saving json file to bucket...')
        filename = f'file.json'
        path_with_filename = path + filename
        bucket, key = self.__decompose_s3_path(s3_path=path_with_filename)
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))
        logging.log(level=logging.INFO, msg='Success!')

        logging.log(level=logging.INFO, msg='Creating table on landing...')
        table_name = path[0:-1].split('/')[-1]
        trino_conn = dbapi.connect(
            host=self.trino_host,
            port=self.trino_port,
            user=self.trino_user,
            catalog=self.trino_catalog,
            schema=self.trino_schema
        )
        query = f"""
        CREATE TABLE IF NOT EXISTS minio.landing.{table_name} (
            tracks ARRAY<ROW(
                album ROW(
                    album_type VARCHAR,
                    artists ARRAY<ROW(
                        external_urls ROW(spotify VARCHAR),
                        href VARCHAR,
                        id VARCHAR,
                        name VARCHAR,
                        type VARCHAR,
                        uri VARCHAR
                    )>,
                    external_urls ROW(spotify VARCHAR),
                    href VARCHAR,
                    id VARCHAR,
                    images ARRAY<ROW(
                        height INTEGER,
                        url VARCHAR,
                        width INTEGER
                    )>,
                    is_playable BOOLEAN,
                    name VARCHAR,
                    release_date VARCHAR,
                    release_date_precision VARCHAR,
                    total_tracks INTEGER,
                    type VARCHAR,
                    uri VARCHAR
                ),
                artists ARRAY<ROW(
                    external_urls ROW(spotify VARCHAR),
                    href VARCHAR,
                    id VARCHAR,
                    name VARCHAR,
                    type VARCHAR,
                    uri VARCHAR
                )>,
                disc_number INTEGER,
                duration_ms INTEGER,
                explicit BOOLEAN,
                external_ids ROW(isrc VARCHAR),
                external_urls ROW(spotify VARCHAR),
                href VARCHAR,
                id VARCHAR,
                is_local BOOLEAN,
                is_playable BOOLEAN,
                linked_from ROW(
                    external_urls ROW(spotify VARCHAR),
                    href VARCHAR,
                    id VARCHAR,
                    type VARCHAR,
                    uri VARCHAR
                ),
                name VARCHAR,
                popularity INTEGER,
                preview_url VARCHAR,
                track_number INTEGER,
                type VARCHAR,
                uri VARCHAR
            )>,
            seeds ARRAY<ROW(
                initialPoolSize INTEGER,
                afterFilteringSize INTEGER,
                afterRelinkingSize INTEGER,
                id VARCHAR,
                type VARCHAR,
                href VARCHAR
            )>
        )
        WITH (
            external_location = '{path}',
            format = 'JSON'
        )
        """

        cursor = trino_conn.cursor()
        cursor.execute(query)
        cursor.close()
        logging.log(level=logging.INFO, msg='Success!')
