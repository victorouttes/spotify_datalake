# Spotify Datalake

Este projeto visa capturar dados de popularidade de músicas de 3 categorias (_J-Rock_, _J-Pop_ e _K-Pop_)
do Spotify e disponibilizar em um dashboard para visualização. A ideia é que não fossem utilizadas bases de dados
prontas da internet, então está é uma solução personalizada e adaptada ao nosso objetivo inicial. Por isto precisei
lidar com alguns desafios inerentes a dados reais, que serão abordados no decorrer deste documento.

Em resumo, quero: 
* Criar um data lake, seguindo a arquitetura _medallion_;
* Utilizar o Trino como query engine para acessar os dados do data lake;
* Coletar dados de popularidade das 3 categorias via API REST do Spotify;
* Armazenar estes dados na camada _landing_;
* Fazer as transformações de dados entre as camadas _bronze_, _silver_ e _gold_ utilizando DBT (_Data Build Tool_) através do Trino;
* Disponibilizar o Apache Superset como ferramenta de DataViz e criar um dashboard acessando os dados na camada _gold_.

Com isto definido, a arquitetura que iremos montar é a seguinte:

![architecture.png](images%2Farchitecture.png)

Ela foi pensada para ser moderna, open source e poder ser executada via Docker, para possibilitar a todos 
rodarem localmente, além de ser agnóstica à clouds.

Principais ferramentas utilizadas no projeto:

* **Apache Airflow**: poderosa e conhecida ferramenta de orquestração de pipelines de dados;
* **DBT**: poderosa ferramenta de transformação e testes de dados;
* **Minio**: ferramenta para _object storage_ gratuita e que utiliza o mesmo protocolo (e bibliotecas!) do AWS S3;
* **Trino**: ferramenta de _query engine_. Possibilita acessar diversas fontes de dados como bancos transacionais, nosql ou mesmo arquivos parquet/csv/json e fazer consultas padrão SQL em cima dessas fontes. Permite inclusive cruzar dados de diferentes fontes de forma transparente;
* **Apache Superset**: ferramenta opensource de visualização de dados. 

Normalmente não é função do engenheiro de dados chegar no nível do DataViz, mas o objetivo aqui é entregar a solução de
ponta a ponta. 

# Construção do Projeto
## Acesso à API do Spotify

A API REST do Spotify permite que desenvolvedores integrem funcionalidades do Spotify em suas aplicações. 
Com ela, você pode acessar uma ampla gama de dados e funcionalidades, desde informações sobre álbuns, 
faixas e playlists até a execução de músicas e a criação de novas playlists. 

Para este projeto vamos utilizar o acesso aos endpoints que não necessitam de autorização do usuário-alvo, 
como, por exemplo, ver as categorias de músicas existentes, ver detalhes de determinado artista, porém não ver 
playlists de determinado usuário, ok?

Para utilizar a API, você precisa conseguir o _Client ID_ e o _Client Secret_. Para tal, você precisa 
ter cadastro no Spotify e criar uma aplicação aqui: https://developer.spotify.com/dashboard. Depois
da aplicação criada, você deve conseguir ter acesso aos 2 itens, conforme imagem abaixo:

![spotify_create_app.png](images%2Fspotify_create_app.png)

Agora vamos criar uma pasta pra nosso projeto. E dentro dela, criar um arquivo _.env_ com essas 
2 variáveis, pois usaremos a seguir:

```
SPOTIFY_CLIENT_ID = valor
SPOTIFY_SECRET = valor
```

## Criando Serviço do Minio (Object Storage)
Agora vamos começar a criar os serviços que utilizaremos. Primeiro vamos criar o _Minio_. Crie um arquivo dentro da pasta
do nosso projeto chamado **docker-compose.yml**. Coloque:

```yaml
version: '3.7'

services:
  minio:
    image: minio/minio
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
  
volumes:
  minio-data:
```

Isto vai subir o serviço do Minio na porta 9000 e estamos criando um volume persistente para os dados chamado _minio-data_.
Executando um `docker-compose up -d` vai possibilitar seu acesso na url `http://localhost:9000/`, onde você pode colocar
as credenciais definidas no nosso docker compose (minioadmin e minioadmin123). Você já pode sair criando buckets e 
adicionando arquivos para ver como funciona, porém, vamos automatizar algumas coisas aqui.

No mesmo arquivo **docker-compose.yml**, atualize para ele ficar assim:

```yaml
version: '3.7'

services:
  minio:
    image: minio/minio
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
  
  minio-mc:
    image: minio/mc:latest
    container_name: minio-mc
    depends_on:
      - minio
    entrypoint: |
      sh -c "
      sleep 10 &&
      mc alias set myminio http://minio:9000 minioadmin minioadmin123 &&
      mc mb myminio/landing &&
      mc mb myminio/bronze &&
      mc mb myminio/silver &&
      mc mb myminio/gold
      "
  
volumes:
  minio-data:
```
Perceba que adicionamos mais um serviço chamado _minio-mc_. Este serviço vai rodar (em seu entrypoint) alguns comandos
para criar todos os nossos buckets (landing, bronze, silver e gold)! Perceba que todos os acessos entre os serviços
daqui para frente serão feitos através do nome do serviço mais a porta interna do serviço, por isto o 
`mc alias set myminio http://minio:9000`. Você não precisa descobrir o IP interno de cada serviço para referenciá-lo.

Agora, faça um `docker-compose down -v`, para derrubar os serviços e deletar os volumes e em seguida outro
`docker-compose up -d`. Agora, quando tudo subir e você acessar novamente `http://localhost:9000/` verá que já vão
existir os 4 buckets que precisamos!

![minio_create_buckets.png](images%2Fminio_create_buckets.png)

## Criando o Serviço do Airflow
Agora adicionaremos em nosso **docker-compose.yml** mais 2 serviços para conseguirmos ter o Airflow. Não vá se perder,
pois vou colocando sempre o nosso **docker-compose.yml** completinho:

```yaml
version: '3.7'

services:
  minio:
    image: minio/minio
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
  
  minio-mc:
    image: minio/mc:latest
    container_name: minio-mc
    depends_on:
      - minio
    entrypoint: |
      sh -c "
      sleep 10 &&
      mc alias set myminio http://minio:9000 minioadmin minioadmin123 &&
      mc mb myminio/landing &&
      mc mb myminio/bronze &&
      mc mb myminio/silver &&
      mc mb myminio/gold
      "
  
  postgres-airflow:
    image: postgres:13
    container_name: postgres-airflow
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  airflow:
    build:
      context: .
      dockerfile: config_airflow/airflow.Dockerfile
    container_name: airflow
    depends_on:
      - minio
      - postgres-airflow
    environment:
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
      AIRFLOW__CORE__PLUGINS_FOLDER: /opt/airflow/plugins
      AIRFLOW__LOGGING__LOGGING_LEVEL: INFO
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ACCESS_KEY: minioadmin
      MINIO_SECRET_KEY: minioadmin123
      SPOTIFY_CLIENT_ID: ${SPOTIFY_CLIENT_ID}
      SPOTIFY_SECRET: ${SPOTIFY_SECRET}
    command: ['airflow', 'standalone']
    ports:
      - "8080:8080"
    volumes:
      - ./airflow/dags:/opt/airflow/dags  
  
volumes:
  minio-data:
```
Primeiro adicionamos um banco de dados _Postgres_ para guardar os metadados do nosso _Airflow_, como credenciais, 
histórico de execuções, conexões, etc.

Depois adicionamos um serviço para o _Airflow_ propriamente dito. Observe que não utilizamos uma imagem diretamente
do DockerHub aqui. Eu precisei criar um Dockerfile para poder incluir algumas bibliotecas Python que vamos precisar,
por isto crie um arquivo na raíz do projeto chamado `requirements.txt` contendo:

```
apache-airflow==2.9.2
dbt==1.0.0.38.4
dbt-trino==1.8.0
trino==0.328.0
boto3==1.34.136
pyarrow==16.1.0
pandas==2.2.2
s3fs
```

Também uma pasta `config_airflow` e, dentro dela, um arquivo chamado `airflow.Dockerfile` contendo:

```dockerfile
FROM apache/airflow:2.9.2-python3.11

COPY requirements.txt .
RUN pip install -r requirements.txt
```
Como você deve ter notado, este _Dockerfile_ vai usar o _Airflow 2.9.2_ como base e instalar o conteúdo do `requirements.txt`.

Você pode utilizar este mesmo arquivo `requirements.txt` para instalar localmente e poder rodar testes com o ambiente
depois.

Também definimos algumas variáveis de ambiente dentro do _Airflow_, as mais importantes aqui são as credenciais do
Minio e os dados de acesso ao Spotify. Se você criar o arquivo `.env` contendo as variáveis lá, o **docker-compose.yml**
conseguirá acessá-las conforme o trecho:

```yaml
SPOTIFY_CLIENT_ID: ${SPOTIFY_CLIENT_ID}
SPOTIFY_SECRET: ${SPOTIFY_SECRET}
```

Aqui, estamos inicializando o _Airflow_ no modo _standalone_, o que significa que não vamos precisar de diversos serviços
acessórios como o _Celery_, _Worker_, etc., porém este modo de execução deve ser usado apenas para estudos.

Por fim, definimos este mapeamento de volume aqui:

```yaml
volumes:
  - ./airflow/dags:/opt/airflow/dags  
```
Que estará mapeando nosso diretório local (crie ele!) `airflow/dags` para a pasta interna `/opt/airflow/dags`, o que
significa que criando ou editando uma dag em nosso sistema de arquivos local refletirá para o _Airflow_ do _container_!

Não vá se perder, por enquanto temos a seguinte estrutura:
```
meu_projeto/
├─ airflow/
│  ├─ dags/
├─ config_airflow/
│  ├─ airflow.Dockerfile
├─ .env
├─ docker-compose.yml
├─ requirements.txt
```

Se você fizer um `docker-compose up -d`, poderá acessar o _Airflow_ em `http://localhost:8080/`. No modo _standalone_,
o usuário será sempre `admin` e a senha é gerada sempre que você iniciar o serviço pela primeira vez. Você pode ter
acesso à senha em um arquivo chamado `standalone_admin_password.txt` e você pode acessar seu conteúdo com:

```
docker exec -it airflow cat standalone_admin_password.txt
```

Assim, você conseguirá logar na ferramenta. Mas ainda não temos nenhuma DAG lá.

## Criando a Dag para Ingestão de Dados
Bom, já temos acesso ao airflow, agora vamos criar nossa primeira DAG. Dentro da pasta `airflow/dags` crie um arquivo
chamado `dag_spotify.py`. Mas, antes de escrevermos a DAG, vamos criar 2 arquivos para nos auxiliar. Dentro da pasta 
`airflow/dags` crie a pasta `util` e mais 2 arquivos dentro: `spotify.py` e `storage.py`. O conteúdo do primeiro deve ser:

```python
import base64
import os

import requests


class SpotifyAPI:
    def __init__(self):
        self.client_id = os.environ.get('SPOTIFY_CLIENT_ID')
        self.client_secret = os.environ.get('SPOTIFY_SECRET')

    def __auth(self) -> str:
        auth_url = 'https://accounts.spotify.com/api/token'
        auth_header = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        headers = {'Authorization': f'Basic {auth_header}'}
        data = {'grant_type': 'client_credentials'}
        response = requests.post(auth_url, headers=headers, data=data)
        response_data = response.json()
        return response_data['access_token']

    def get_top_songs_recommendation(self, genre: str) -> dict:
        assert genre in ['anime', 'j-rock', 'j-pop', 'k-pop']
        token = self.__auth()
        api_url = f'https://api.spotify.com/v1/recommendations'
        headers = {'Authorization': f'Bearer {token}'}
        params = {'seed_genres': genre, 'market': 'BR', 'min_popularity': 40, 'limit': 100}
        response = requests.get(api_url, headers=headers, params=params)
        return response.json()
```
Aqui implementamos uma classe que expõe o método `get_top_songs_recommendation()`. Ele serve pra acessar o endpoint de
recomendações de músicas do Spotify e estamos aplicando filtros de gênero musical, mercado (brasileiro) e valor mínimo de
popularidade (uma métrica do Spotify que varia de 0 a 100 e quanto maior, mais popular).

O segundo arquivo facilita a criação de arquivos em nosso data lake (o _Minio_):

```python
import json
import logging
import os
from urllib.parse import urlparse

import boto3


class Storage:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            region_name='us-east-1',
            endpoint_url=os.environ.get('MINIO_ENDPOINT'),
            aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY')
        )

    def __decompose_s3_path(self, s3_path: str) -> tuple:
        parsed_url = urlparse(s3_path)
        bucket = parsed_url.netloc
        key = parsed_url.path.lstrip('/')
        return bucket, key

    def save_top_songs_recommendation_to_bucket(self, data: dict, path: str):
        logging.log(level=logging.INFO, msg='Saving json file to bucket...')
        filename = f'file.json'
        path_with_filename = path + filename
        bucket, key = self.__decompose_s3_path(s3_path=path_with_filename)
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))
        logging.log(level=logging.INFO, msg='Success!')
```
Temos mais uma classe, onde o método mais importante aqui é o `save_top_songs_recommendation_to_bucket()`, que recebe
o json original vindo da API REST do _Spotify_ e o salva no data lake. O JSON vindo da API é um tanto complexo, mas veremos
mais sobre isto quando precisarmos acessá-lo via o nosso _query engine_.

Agora sim, podemos escrever o código em nossa DAG:

```python
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from util.spotify import SpotifyAPI
from util.storage import Storage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_spotify',
    default_args=default_args,
    description='Spotify data lake',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['spotify', 'landing'],
) as dag:
    with TaskGroup(group_id='spotify_ingestion') as spotify_ingestion:
        def ingestion(genre: str, s3_path: str):
            api = SpotifyAPI()
            json_data = api.get_top_songs_recommendation(genre=genre)
            storage = Storage()
            storage.save_top_songs_recommendation_to_bucket(data=json_data, path=s3_path)

        PythonOperator(
            task_id='spotify_jrock_ingestion',
            python_callable=ingestion,
            op_kwargs={'genre': 'j-rock', 's3_path': 's3a://landing/spotify_recommend_tracks_jrock/'},
        )
        PythonOperator(
            task_id='spotify_jpop_ingestion',
            python_callable=ingestion,
            op_kwargs={'genre': 'j-pop', 's3_path': 's3a://landing/spotify_recommend_tracks_jpop/'},
        )
        PythonOperator(
            task_id='spotify_kpop_ingestion',
            python_callable=ingestion,
            op_kwargs={'genre': 'k-pop', 's3_path': 's3a://landing/spotify_recommend_tracks_kpop/'},
        )
```
Aqui criamos um _TaskGroup_ para a ingestão de dados, com 3 _tasks_, uma para cada gênero (_J-Rock_, _J-Pop_ e _K-Pop_).
Também passamos o path no nosso data lake onde queremos salvar os dados que serão coletados. Cada task é criada usando o
básico _PythonOperator_, chamando a função `ingestion()`, que por sua vez chama as 2 classes que acabamos de criar,
para acessar o Spotify e obter os dados e a outra para salvar os dados em formato JSON no _Minio_.

Se olharmos em nosso Airflow agora e mandarmos executar a DAG, deveremos ter algo assim:

![airflow_dag_ingestion.png](images%2Fairflow_dag_ingestion.png)

Tudo dando certo, teremos isso em nosso _object storage_:

![minio_landing.png](images%2Fminio_landing.png)

Ficamos com nossa estrutura de arquivos do projeto assim:
```
meu_projeto/
├─ airflow/
│  ├─ dags/
│  │  ├─ util/
│  │  │  ├─ spotify.py
│  │  │  ├─ storage.py
│  │  ├─ dag_spotify.py
├─ config_airflow/
│  ├─ airflow.Dockerfile
├─ .env
├─ docker-compose.yml
├─ requirements.txt
```

## Criando o Serviço do Trino
Precisamos acessar os dados do nosso data lake e para isto usaremos o _Trino_. Vamos precisar de muitos
serviços agora, vamos começar com o básico. Adicione estes serviços no nosso **docker-compose.yml**:

```yaml
  trino-coordinator:
    container_name: trino
    image: trinodb/trino:latest
    hostname: trino-coordinator
    environment:
      - TRINO_ENVIRONMENT=production
    ports:
      - 8085:8080
    depends_on:
      - minio-mc
    volumes:
      - ./config_trino:/etc/trino

  trino-worker:
    image: trinodb/trino:latest
    container_name: trino-worker
    hostname: trino-worker
    environment:
      - TRINO_ENVIRONMENT=production
      - TRINO_DISCOVERY_URI=http://trino-coordinator:8080
    depends_on:
      - trino-coordinator
    volumes:
      - ./config_trino:/etc/trino
```
O primeiro é o coordenador e interface de acesso ao Trino, o segundo é o que vai executar as tarefas.
Aqui mapeamos o mesmo mapeamento nos 2 serviços `./config_trino:/etc/trino`. O conteúdo da pasta local
`config_trino` será criado em breve. Isto é o suficiente para iniciarmos o Trino. 
Porém, ainda precisamos indicar qual o _MetaStore_ vamos utilizar nele. Utilizaremos o famoso _Hive_. 
Vamos adicionar mais 2 serviços então:

```yaml
  trino-coordinator:
    container_name: trino
    image: trinodb/trino:latest
    hostname: trino-coordinator
    environment:
      - TRINO_ENVIRONMENT=production
    ports:
      - 8085:8080
    depends_on:
      - minio-mc
    volumes:
      - ./config_trino:/etc/trino

  trino-worker:
    image: trinodb/trino:latest
    container_name: trino-worker
    hostname: trino-worker
    environment:
      - TRINO_ENVIRONMENT=production
      - TRINO_DISCOVERY_URI=http://trino-coordinator:8080
    depends_on:
      - trino-coordinator
    volumes:
      - ./config_trino:/etc/trino

  mariadb:
    container_name: mariadb
    hostname: mariadb
    image: mariadb:10.5.8
    ports:
      - 3307:3306
    environment:
      - MYSQL_ROOT_PASSWORD=admin
      - MYSQL_USER=admin
      - MYSQL_PASSWORD=admin
      - MYSQL_DATABASE=metastore_db

  hive-metastore:
    container_name: hive-metastore
    hostname: hive-metastore
    image: 'bitsondatadev/hive-metastore:latest'
    ports:
      - 9083:9083 # Metastore Thrift
    volumes:
      - ./config_hive/metastore-site.xml:/opt/apache-hive-metastore-3.0.0-bin/conf/metastore-site.xml:ro
    environment:
      - METASTORE_DB_HOSTNAME=mariadb
    depends_on:
      - mariadb
```
Começamos com um banco _MariaDB_ para armazenar os metadados do _Hive_, depois adicionamos o próprio _Hive_.
No _Hive_ adicionamos um *.xml de configuração, contendo:

```xml
<configuration>
    <property>
        <name>metastore.thrift.uris</name>
        <value>thrift://hive-metastore:9083</value>
        <description>Thrift URI for the remote metastore. Used by metastore client to connect to remote metastore.</description>
    </property>
    <property>
        <name>metastore.task.threads.always</name>
        <value>org.apache.hadoop.hive.metastore.events.EventCleanerTask,org.apache.hadoop.hive.metastore.MaterializationsCacheCleanerTask</value>
    </property>
    <property>
        <name>metastore.expression.proxy</name>
        <value>org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>com.mysql.cj.jdbc.Driver</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:mysql://mariadb:3306/metastore_db</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>admin</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>admin</value>
    </property>

    <property>
        <name>fs.s3a.access.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>minioadmin123</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
</configuration>
```
Que deve ser criado na pasta `config_hive/metastore-site.xml`. Este arquivo configura algumas coisas, incluindo o acesso
ao nosso object storage e ao nosso banco de metadados (MariaDB).

Lembra da pasta `config_trino`? Agora vamos criar alguns arquivos nela, para amarrar o Trino com o MetaStore, além de fazer
mais algumas configurações necessárias! Primeiro crie a pasta `config_trino` e depois crie os arquivos a seguir:

`config.properties`
```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery-server.enabled=true
discovery.uri=http://trino-coordinator:8080
```

`jvm.config`
```properties
-server
-Xmx1G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+UseGCOverheadLimit
-XX:+ExitOnOutOfMemoryError
-XX:ReservedCodeCacheSize=256M
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-XX:+UnlockDiagnosticVMOptions
-XX:G1NumCollectionsKeepPinned=10000000
```

`node.properties`
```properties
node.environment=docker
node.data-dir=/data/trino
plugin.dir=/usr/lib/trino/plugin
```

Agora, crie uma pasta `catalog` dentro de `config_trino` e, nela, o arquivo:

`minio.properties`
```properties
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.path-style-access=true
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin123
hive.non-managed-table-writes-enabled=true
```

Neste último estamos linkando tudo: _Trino_ com _Hive_ com _Minio_!

Só precisamos de mais um detalhe agora: queremos que o _Trino_ já crie os _schemas_ automaticamente pra nós, referente às
camadas _landing_, _bronze_, _silver_ e _gold_! O objetivo é que os dados do bucket _landing_ apareçam no _schema_ _landing_
e assim por diante. Para isto, vamos adicionar um pequeno serviço apenas para criar os _schemas_ pra nós:

```yaml
  trino-coordinator:
    container_name: trino
    image: trinodb/trino:latest
    hostname: trino-coordinator
    environment:
      - TRINO_ENVIRONMENT=production
    ports:
      - 8085:8080
    depends_on:
      - minio-mc
    volumes:
      - ./config_trino:/etc/trino

  trino-worker:
    image: trinodb/trino:latest
    container_name: trino-worker
    hostname: trino-worker
    environment:
      - TRINO_ENVIRONMENT=production
      - TRINO_DISCOVERY_URI=http://trino-coordinator:8080
    depends_on:
      - trino-coordinator
    volumes:
      - ./config_trino:/etc/trino

  trino-init:
    image: trinodb/trino:latest
    depends_on:
      - trino-coordinator
    entrypoint: [ "/bin/sh", "-c", "sleep 30 && trino --server trino-coordinator:8080 -f /docker-entrypoint-initdb.d/create_schemas.sql" ]
    volumes:
      - ./init_trino/create_schemas.sql:/docker-entrypoint-initdb.d/create_schemas.sql  

  mariadb:
    container_name: mariadb
    hostname: mariadb
    image: mariadb:10.5.8
    ports:
      - 3307:3306
    environment:
      - MYSQL_ROOT_PASSWORD=admin
      - MYSQL_USER=admin
      - MYSQL_PASSWORD=admin
      - MYSQL_DATABASE=metastore_db

  hive-metastore:
    container_name: hive-metastore
    hostname: hive-metastore
    image: 'bitsondatadev/hive-metastore:latest'
    ports:
      - 9083:9083 # Metastore Thrift
    volumes:
      - ./config_hive/metastore-site.xml:/opt/apache-hive-metastore-3.0.0-bin/conf/metastore-site.xml:ro
    environment:
      - METASTORE_DB_HOSTNAME=mariadb
    depends_on:
      - mariadb
```
O serviço `trino-init` vai rodar o arquivo `create_schemas.sql` que vamos criar na pasta `init_trino` que fará o serviço.
Portanto, crie a pasta e o arquivo `init_trino/create_schemas.sql`, contendo:

```
create schema if not exists minio.landing with (location = 's3a://landing/');
create schema if not exists minio.prod_bronze with (location = 's3a://bronze/');
create schema if not exists minio.prod_silver with (location = 's3a://silver/');
create schema if not exists minio.prod_gold with (location = 's3a://gold/');
```

Ficamos com a estrutura de arquivos agora:
```
meu_projeto/
├─ airflow/
│  ├─ dags/
│  │  ├─ dbt_project/
│  │  ├─ util/
│  │  │  ├─ spotify.py
│  │  │  ├─ storage.py
│  │  ├─ dag_spotify.py
├─ config_airflow/
│  ├─ airflow.Dockerfile
├─ config_hive/
│  ├─ metastore-site.xml
├─ config_trino/
│  ├─ catalog/
│  │  ├─ minio.properties
│  ├─ config.properties
│  ├─ jvm.config
│  ├─ node.properties
├─ init_trino/
│  ├─ create_schemas.sql
├─ .env
├─ docker-compose.yml
├─ requirements.txt
```

Bastante coisa, não é? Se você derrubar os serviços com um `docker-compose down -v` e subir novamente, com `docker-compose up -d`,
terá tudo criado automáticamente. Seu _Minio_ terá os buckets criados e o Trino aparecerá com os _schemas_ como na imagem:

![trino_dbeaver.png](images%2Ftrino_dbeaver.png)

Ah, aqui estou usando o _Dbeaver_ para acessar o _Trino_. Você pode configurá-lo assim:

![trino_config_dbeaver.png](images%2Ftrino_config_dbeaver.png)

Não precisa de senha! Atenção na porta, que é 8085 (se você seguiu todo o tutorial exatamente como está escrito).

## Modificando a DAG de Ingestão
Até aqui nós temos um serviço de object storage funcionando, o query engine funcionando e nosso orquestrador de pipelines
funcionando e gerando arquivos dentro do data lake. Porém, para cada tabela criada no data lake, nós precisamos
indicar ao _Trino_ onde estão estes arquivos e qual a estrutura deles (colunas e tipos de dado). Isto é feito atráves da
criação de tabelas no _Trino_. Os metadados dessas tabelas serão armazenados no nosso serviço _Hive_. Ou seja, por mais
que tenhamos escrito arquivos (JSON) no nosso data lake (_Minio_), eles ainda não serão visíveis no _Trino_ até que façamos
a criação das tabelas lá.

Então vamos ajustar nossa DAG de ingestão de dados para fazer isto. Na verdade, precisamos ajustar o arquivo `airflow/dags/util/storage.py`:

```python
import json
import logging
import os
from urllib.parse import urlparse

import boto3
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
```
A primeira mudança é informar as credenciais de acesso ao _Trino_. Fazemos isso em:
```python
self.trino_host = 'trino-coordinator'
self.trino_port = 8080
self.trino_user = 'admin'
self.trino_catalog = 'minio'
self.trino_schema = 'landing'
```
A segunda coisa é, após salvar o arquivo JSON no _Minio_, devemos criar uma tabela no _Trino_ com a estrutura de dados
deste JSON. Como eu já havia dito antes, é um JSON um tanto complexo. Não vou entrar em detalhes aqui de como se monta
uma tabela no _Trino_ mapeando um JSON, mas basta saber que é bem similar a como seria um Parquet ou CSV. Porém um JSON
possui estruturas como listas e outros JSON aninhados, o que exige uma ampliação nos tipos de dados. No fim da query de
criação da tabela, eu indico a localização da pasta com os arquivos no data lake e o formato deles também, que pode ser
visto no trecho:
```
WITH (
    external_location = '{path}',
    format = 'JSON'
)
```
Também note que estou criando a tabela no schema _landing_:
```
CREATE TABLE IF NOT EXISTS minio.landing.{table_name}
```
Agora, cada vez que a DAG rodar, além de adicionar o JSON no bucket _landing_, ainda vai criar (se não existe ainda) a tabela
no _Trino_. Assim você vai conseguir ver as tabelas ingeridas em _landing_:

![trino_landing_tabelas.png](images%2Ftrino_landing_tabelas.png)

E poderá acessar esses dados assim:

![trino_query_landing.png](images%2Ftrino_query_landing.png)

## Transformações nos Dados com DBT
O DBT (_Data Build Tool_) é uma poderosa ferramenta para fazer transformação nos dados do seu data lake ou _datawarehouse_.
Neste tutorial não vou entrar em detalhes sobre a ferramenta, vou assumir que você já conhece o básico e sabe criar e configurar
um projeto.

Vamos criar nosso projeto DBT dentro da pasta `airflow/dags/`, por comodidade na hora do acesso futuro via _Airflow_.
Vamos usar o plugin `dbt-trino` para acessar o _Trino_ e você já deve estar com tudo instalado, pois está no **requirements.txt**.

Agora precisamos configurar algumas coisas no DBT. Aqui, chamei o projeto de `dbt_project`. Crie um arquivo `profiles.yml`
na raíz do projeto DBT com o conteúdo:

```yaml
dbt_project:
  target: prod
  outputs:
    prod:
      type: trino
      threads: 4
      host: trino-coordinator
      port: 8080
      user: admin
      catalog: minio
      schema: prod
      connection_method: direct
```
Aqui vai servir pra o DBT conseguir se conectar com o Trino para poder escrever novas tabelas lá, via SQL. Importante notar
a porta utilizada, que é a 8080. Esta é a porta interna do nosso container, não a porta externa (8085), que usamos para acessar
pelo DBeaver.

No arquivo `dbt_project.yml`, deixe a seção **models** desta forma:
```yaml
models:
  dbt_project:
    01_bronze:
      materialized: table
      +schema: bronze
    02_silver:
      materialized: table
      +schema: silver
    03_gold:
      materialized: table
      +schema: gold
```

Agora, vamos começar a criar nossos modelos de transformação! Dentro da pasta models você vai criar as 3 pastas:
* 01_bronze
* 02_silver
* 03_gold

e o arquivo `sources.yml`. Este yml deve conter o mapeamento da nossa fonte de dados pro DBT, que será nossa camada
_landing_:

```yaml
version: 2

sources:
  - name: landing
    catalog: minio
    schema: landing
    tables:
      - name: spotify_recommend_tracks_jrock
      - name: spotify_recommend_tracks_jpop
      - name: spotify_recommend_tracks_kpop
```

Dentro de cada pasta que criamos, você poderá fazer a transformação que quiser para levar os dados de _landing_ para _bronze_,
de _bronze_ para _silver_ e de _silver_ para _gold_.

A query para levar os dados de _landing_ para _bronze_ vai ser a mais complexa, visto a natureza do dado ser JSON. Aqui vai um
exemplo de transformação de uma das tabelas de _landing_ para _bronze_:
```
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
```

Estamos utilizando o tipo de armazenamento físico do dado (no Minio) como _iceberg_ e a consulta num JSON com listas precisa
ser feito com a ajuda de um `cross join unnest`. O resultado dessa consulta especificamente é (você pode rodar no Dbeaver,
apenas trocando o `{{ source('landing', 'spotify_recommend_tracks_jpop') }}` pelo nome real da tabela):

![trino_query_landing_testes.png](images%2Ftrino_query_landing_testes.png)

Você deve criar os modelos para as 3 tabelas de _landing_.

Na pasta dos modelos de silver, eu resolvi criar um único modelo, com o union das 3 tabelas de bronze:

```
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
```

Já em gold, eu criei 5 modelos, um pra cada indicador/chart que eu queria criar. Eu também criei testes de
qualidade para todos os modelos, em todas as camadas. Como, por exemplo, o arquivo `airflow/dags/dbt_project/models/02_silver/silver_spotify_recommendations.yml`:

```yaml
version: 2

models:
  - name: silver_spotify_recommendations
    columns:
    - name: song_id
      data_tests:
        - not_null
    - name: song_name
      data_tests:
        - not_null
    - name: song_popularity
      data_tests:
        - not_null
        - dbt_utils.accepted_range:
            min_value: 0
            max_value: 100
    - name: genre
      data_tests:
        - not_null
        - accepted_values:
            values: ['J-Rock', 'J-Pop', 'K-Pop']

```
Você pode rodar fora do container o projeto DBT, mas precisa trocar (no `profiles.yml`) o `host`para `localhost`
e a `port` para `8085`. Fazendo isso e rodando um `dbt deps && dbt build --profiles-dir .` você terá o seguinte efeito no
_Trino_:

![trino_completo.png](images%2Ftrino_completo.png)

Os arquivos dos modelos podem ser vistos neste projeto, em seus respectivos diretórios. Você pode criar outros também,
para estudar.

## Ajustando a DAG para Executar o DBT
Até aqui nós já geramos todas as tabelas necessárias no nosso data lake e já temos o ambiente de _gold_ populado e pronto
para o analista construir os dashboards. Porém, nós executamos o DBT manualmente e agora vamos fazer um pequeno ajuste na 
nossa DAG do _Airflow_ para conseguir rodar o DBT automaticamente durante o pipeline de dados.

Adicione o seguinte TaskGroup na nossa dag `dag_spotify.py`:

```python
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from util.spotify import SpotifyAPI
from util.storage import Storage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_spotify',
    default_args=default_args,
    description='Spotify data lake',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['spotify', 'landing'],
) as dag:
    with TaskGroup(group_id='spotify_ingestion') as spotify_ingestion:
        def ingestion(genre: str, s3_path: str):
            api = SpotifyAPI()
            json_data = api.get_top_songs_recommendation(genre=genre)
            storage = Storage()
            storage.save_top_songs_recommendation_to_bucket(data=json_data, path=s3_path)

        PythonOperator(
            task_id='spotify_jrock_ingestion',
            python_callable=ingestion,
            op_kwargs={'genre': 'j-rock', 's3_path': 's3a://landing/spotify_recommend_tracks_jrock/'},
        )
        PythonOperator(
            task_id='spotify_jpop_ingestion',
            python_callable=ingestion,
            op_kwargs={'genre': 'j-pop', 's3_path': 's3a://landing/spotify_recommend_tracks_jpop/'},
        )
        PythonOperator(
            task_id='spotify_kpop_ingestion',
            python_callable=ingestion,
            op_kwargs={'genre': 'k-pop', 's3_path': 's3a://landing/spotify_recommend_tracks_kpop/'},
        )

    with TaskGroup(group_id='spotify_transformation') as spotify_transformation:
        dbt_build = BashOperator(
            task_id='dbt_build',
            bash_command='cd /opt/airflow/dags/dbt_project && dbt deps && dbt build --profiles-dir .',
        )

    spotify_ingestion >> spotify_transformation
```
Via `BashOperator` nós entramos na pasta do projeto DBT e executamos os mesmos comandos que fizemos manualmente. Aqui,
sugiro que você inicie uma execução nova de todo o projeto com `docker-compose down -v` e `docker-compose up -d` e 
rode a DAG que acabamos de modificar. 

![airflow_dag_completa.png](images%2Fairflow_dag_completa.png)

## Criando o Serviço Apache Superset
Estamos com toda a parte de engenharia pronta! A cereja do bolo vai ser criar o serviço para o Superset e fazer um
dashboard bem simples para mostrar como acessamos os dados do data lake.

Vamos precisar adicionar mais 3 serviços para utilizar o Superset:
```yaml
  redis:
    image: redis:latest
    container_name: redis
    ports:
      - 6379:6379

  superset-db:
    image: postgres:13
    container_name: superset-db
    environment:
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: superset
      POSTGRES_DB: superset
    volumes:
      - superset-db-data:/var/lib/postgresql/data

  superset:
    build:
      context: .
      dockerfile: ./config_superset/superset.Dockerfile
    container_name: superset
    environment:
      SUPERSET_CONFIG_PATH: /app/pythonpath/superset_config.py
      PYTHONPATH: /app/pythonpath
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: superset
      POSTGRES_DB: superset
      POSTGRES_HOST: superset-db
    volumes:
      - ./config_superset:/app/pythonpath
      - superset-data:/app/superset_home
    ports:
      - 8088:8088
    depends_on:
      - redis
      - superset-db
    entrypoint:
      - sh
      - -c
      - |
        superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin && \
        superset db upgrade && \
        superset init && \
        superset run --host 0.0.0.0 -p 8088 --with-threads --reload
```
O _Superset_ precisa do _Redis_ para cache e de um banco para os metadados, onde escolhi o _Postgres_. Novamente, estou usando
uma imagem personalizada, então você deve criar o arquivo (e a pasta): `config_superset/superset.Dockerfile` contendo:
```dockerfile
FROM apache/superset:latest

USER root
RUN pip install sqlalchemy-trino

USER superset
```

Apenas para instalar o suporte ao _Trino_. Personalizei o _entrypoint_ para poder criar o primeiro usuário automaticamente. 
Também mapeei a pasta `config_superset` para uma pasta interna no _container_, para poder disponibilizar no _Superset_ 
o arquivo `superset_config.py` contendo:

```python
# Celery configuration
class CeleryConfig(object):
    broker_url = 'redis://redis:6379/0'
    result_backend = 'redis://redis:6379/0'
    worker_log_server = False


CELERY_CONFIG = CeleryConfig

SQLALCHEMY_TRACK_MODIFICATIONS = False

# Superset metadata database configuration
SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://superset:superset@superset-db:5432/superset'

# Additional configurations for Superset
SECRET_KEY = 'ahyeha9182aNah98A'
CSRF_ENABLED = True

# Configuration to support large queries and dashboards
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': 'redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
    'CACHE_REDIS_URL': 'redis://redis:6379/1'
}
```
Este arquivo vai ser carregado na inicialização do _Superset_ e deixar tudo configurado. Após a inicialização, você poderá
se autenticar com `admin` como login e senha:

![superset_login.png](images%2Fsuperset_login.png)

Lá, você poderá configurar a conexão com o _Trino_:

![superset_trino.png](images%2Fsuperset_trino.png)

E poderá criar seus dashboards! Aqui um exemplo simples que fiz para demonstrar, acessando
apenas as tabelas no _schema_ _gold_:

![superset.jpg](images%2Fsuperset.jpg)

Se você gostou deste conteúdo, compartilhe e favorite o repositório! Obrigado!