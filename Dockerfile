FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install cassandra-driver kafka-python

USER 1001

WORKDIR /app