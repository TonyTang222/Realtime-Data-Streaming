#!/usr/bin/env python3
"""
Spark Streaming Entry Point

Simplified entry file; actual logic is in src/consumers/spark_consumer.py

Usage:
    python spark_stream.py

Or run as a module:
    python -m src.consumers.spark_consumer
"""

from src.consumers.spark_consumer import SparkStreamingConsumer


if __name__ == "__main__":
    with SparkStreamingConsumer() as consumer:
        consumer.run()
