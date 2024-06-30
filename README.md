# Spotify Datalake

Este projeto tem como objetivo capturar dados de popularidade de músicas de 3 categorias (_J-Rock_, _J-Pop_ e _K-Pop_)
do Spotify e disponibilizar em um dashboard para visualização. A ideia é que não fossem utilizadas bases de dados
prontas da internet, então está é uma solução personalizada e adaptada ao nosso objetivo inicial. Por isto precisei
lidar com alguns desafios inerentes a dados reais, que serão abordados no decorrer deste documento.

Em resumo, a ideia é: 
* Criar um data lake, seguindo a arquitetura _medallion_;
* Utilizar o Trino como query engine para acessar os dados do data lake;
* Coletar dados de popularidade das 3 categorias via API REST do Spotify;
* Armazenar estes dados na camada _landing_;
* Fazer as transformações de dados entre as camadas _bronze_, _silver_ e _gold_ utilizando DBT (_Data Build Tool_) através do Trino;
* Disponibilizar o Apache Superset como ferramenta de DataViz e criar um dashboard acessando os dados na camada _gold_.

Com isto definido, a arquitetura que iremos montar é a seguinte:

![architecture.png](images%2Farchitecture.png)

Ela foi pensada para ser moderna, open source e poder ser executada via Docker, para possibilitar a todos 
rodarem localmente, além de ser agnóstica à cloud.

Principais ferramentas utilizadas no projeto:

* **Apache Airflow**: poderosa e conhecida ferramenta de orquestração de pipelines de dados;
* **Minio**: ferramenta para _object storage_ gratuita e que utiliza o mesmo protocolo (e bibliotecas!) do AWS S3;
* **Trino**: ferramenta de _query engine_. Possibilita acessar diversas fontes de dados como bancos transacionais, nosql ou mesmo arquivos parquet/csv/json e fazer consultas padrão SQL em cima dessas fontes. Permite inclusive cruzar dados de diferentes fontes de forma transparente;
* **Apache Superset**: ferramenta opensource de visualização de dados. 

Normalmente não é função do engenheiro de dados chegar no nível do DataViz, mas o objetivo aqui é entregar a solução de
ponta à ponta. 

# Construção do Projeto
## Acesso à API do Spotify

A API REST do Spotify permite que desenvolvedores integrem funcionalidades do Spotify em suas aplicações. 
Com ela, você pode acessar uma ampla gama de dados e funcionalidades, desde informações sobre álbuns, 
faixas e playlists até a execução de músicas e a criação de novas playlists. 

Para este projeto vamos utilizar o acesso aos endpoints que não necessitam de autorização do usuário-alvo, 
como por exemplo ver as categorias de músicas existentes, ver detalhes de determinado artista, porém não ver 
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
Agora vamos começar a criar os serviços que utilizaremos. Primeiro vamos criar o Minio. Crie um arquivo dentro da pasta
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
adicionando arquivos para ver como funciona, porém vamos automatizar algumas coisas aqui.

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
daqui pra frente serão feitos através do nome do serviço mais a porta interna do serviço, por isto o 
`mc alias set myminio http://minio:9000`. Você não precisa descobrir o IP interno de cada serviço para referenciá-lo.

Agora, faça um `docker-compose down -v`, para derrubar os serviços e deletar os volumes e em seguida outro
`docker-compose up -d`. Agora, quando tudo subir e você acessar novamente `http://localhost:9000/` verá que já vão
existir os 4 buckets que precisamos!

## Criando o Serviço do Airflow
Agora adicionaremos em nosso **docker-compose.yml** mais 2 serviços para conseguirmos ter o Airflow. Não vá se perder
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
acessórios como o _Celery_, _Worker_, etc. Porém, este modo de execução deve ser usado apenas para estudos.

Por fim, definimos este mapeamento de volume aqui:

```yaml
volumes:
  - ./airflow/dags:/opt/airflow/dags  
```
Que estará mapeando nosso diretório local (crie ele!) `airflow/dags` para a pasta interna `/opt/airflow/dags`, o que
significa que criando ou editando uma dag em nosso sistema de arquivos local refletirá para o _Airflow_ do _container_!

Não vá se perder, por enquanto temos a seguinte estrutura:
```
- airflow/
--- dags/
----- <vazia>
- config_airflow/
--- airflow.Dockerfile
- .env
- docker-compose.yml
- requirements.txt
```