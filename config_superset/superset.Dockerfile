FROM apache/superset:latest

USER root
RUN pip install sqlalchemy-trino

USER superset
