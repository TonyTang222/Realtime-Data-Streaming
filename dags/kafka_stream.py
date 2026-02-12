"""Airflow DAG for Streaming User Data to Kafka."""

import sys
import time
import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add src to Python path for Airflow imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import get_settings
from src.utils.logging_config import setup_logging, get_logger
from src.producers.api_client import RandomUserAPIClient
from src.producers.kafka_producer import ResilientKafkaProducer
from src.transformers.user_transformer import UserTransformer
from src.config.schemas import validate_api_response
from src.exceptions.custom_exceptions import (
    APIError,
    TransformationError,
    DataValidationError
)

setup_logging()
logger = get_logger(__name__)

default_args = {
    'owner': 'Tony',
    'start_date': datetime(2025, 5, 20, 0, 0),
    'retries': 1,
    'retry_delay': 60,
}


def stream_data():
    """
    Stream data from API to Kafka.

    1. Initialize components (API client, Kafka producer, Transformer)
    2. Run continuously for the configured duration
    3. Each iteration: fetch API data -> validate -> transform -> send to Kafka
    4. Record metrics and logs
    5. Send validation failures to DLQ
    """
    settings = get_settings()

    # Stats
    success_count = 0
    failure_count = 0

    logger.info(
        "Starting streaming task",
        extra={
            "duration_seconds": settings.streaming_duration_seconds,
            "kafka_topic": settings.kafka.topic_user_data
        }
    )

    with RandomUserAPIClient() as api_client, \
         ResilientKafkaProducer() as producer:

        transformer = UserTransformer()
        start_time = time.time()
        end_time = start_time + settings.streaming_duration_seconds

        while time.time() < end_time:
            try:
                # 1. Fetch data from API
                api_response = api_client.get_random_user()

                # 2. Validate API response
                is_valid, validated_data, errors = validate_api_response(api_response)
                if not is_valid:
                    logger.warning(
                        "API response validation failed",
                        extra={"errors": errors}
                    )
                    metrics.increment("validation_failures")
                    # Send validation failures to DLQ
                    producer._send_to_dlq(
                        original_data=api_response,
                        source_topic=settings.kafka.topic_user_data,
                        error=DataValidationError(f"Validation failed: {errors}"),
                        retry_count=0
                    )
                    failure_count += 1
                    continue

                # 3. Transform data
                with metrics.timer("transformation"):
                    # Extract raw data (Pydantic model -> dict)
                    raw_user = api_response['results'][0]
                    transformed = transformer.transform(raw_user)

                # 4. Send to Kafka
                with metrics.timer("kafka_send"):
                    success = producer.send(transformed)

                if success:
                    success_count += 1
                    metrics.increment("messages_sent")
                    logger.debug(
                        "Message sent successfully",
                        extra={
                            "user_id": transformed['id'],
                            "username": transformed['username']
                        }
                    )
                else:
                    failure_count += 1
                    metrics.increment("messages_failed")

                # Throttle send rate (~1 message per second)
                time.sleep(1)

            except APIError as e:
                logger.error(
                    f"API error: {e}",
                    extra={"error_type": type(e).__name__}
                )
                metrics.increment("api_errors")
                failure_count += 1
                # API errors skip DLQ (no data to send)
                continue

            except TransformationError as e:
                logger.error(
                    f"Transformation error: {e}",
                    extra={"error_type": type(e).__name__}
                )
                metrics.increment("transformation_errors")
                failure_count += 1
                continue

            except Exception as e:
                logger.error(
                    f"Unexpected error: {e}",
                    extra={"error_type": type(e).__name__}
                )
                metrics.increment("unexpected_errors")
                failure_count += 1
                continue

    # Task complete, log statistics
    duration = time.time() - start_time
    logger.info(
        "Streaming task completed",
        extra={
            "duration_seconds": duration,
            "success_count": success_count,
            "failure_count": failure_count,
            "success_rate": success_count / (success_count + failure_count) if (success_count + failure_count) > 0 else 0
        }
    )

    # Output metrics summary
    metrics.log_summary()


# DAG definition
with DAG(
    'user_auto_loading',
    default_args=default_args,
    description='Stream random user data from API to Kafka',
    schedule_interval='@daily',
    catchup=False,
    tags=['streaming', 'kafka', 'user_data']
) as dag:

    streaming_task = PythonOperator(
        task_id='streaming_data_from_api',
        python_callable=stream_data,
        doc_md="""
        ## Streaming Data Task

        This task:
        1. Fetches random users from randomuser.me API
        2. Validates and transforms the data
        3. Sends to Kafka topic 'user_data'

        **Duration**: Configurable via STREAMING_DURATION_SECONDS env var (default: 60s)

        **Error Handling**:
        - API errors: Logged and retried
        - Validation errors: Sent to DLQ
        - Kafka errors: Retried with exponential backoff, then DLQ
        """
    )

    # Additional tasks can be added here, e.g.:
    # validate_task >> streaming_task >> notify_task
