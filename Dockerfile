FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    python3-dev \
    pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install cassandra-driver kafka-python pydantic python-dotenv

WORKDIR /app