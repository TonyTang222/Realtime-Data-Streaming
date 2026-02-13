"""Configuration Management with Environment Variables"""

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Environment(Enum):
    """Execution environment."""

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class KafkaConfig:
    """Kafka configuration."""

    # Broker connection
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    )

    # Topic names
    topic_user_data: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_USER_DATA", "user_data")
    )
    topic_dlq: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_DLQ", "user_data_dlq")
    )

    # Producer settings
    acks: str = field(default_factory=lambda: os.getenv("KAFKA_ACKS", "all"))
    retries: int = field(default_factory=lambda: int(os.getenv("KAFKA_RETRIES", "3")))
    max_block_ms: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_MAX_BLOCK_MS", "5000"))
    )

    # Batching
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_BATCH_SIZE", "16384"))
    )
    linger_ms: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_LINGER_MS", "10"))
    )


@dataclass
class CassandraConfig:
    """Cassandra configuration."""

    hosts: str = field(
        default_factory=lambda: os.getenv("CASSANDRA_HOSTS", "cassandra")
    )
    port: int = field(default_factory=lambda: int(os.getenv("CASSANDRA_PORT", "9042")))
    keyspace: str = field(
        default_factory=lambda: os.getenv("CASSANDRA_KEYSPACE", "spark_streams")
    )
    table: str = field(
        default_factory=lambda: os.getenv("CASSANDRA_TABLE", "created_users")
    )
    replication_factor: int = field(
        default_factory=lambda: int(os.getenv("CASSANDRA_REPLICATION_FACTOR", "1"))
    )
    # Authentication (optional)
    username: Optional[str] = field(
        default_factory=lambda: os.getenv("CASSANDRA_USERNAME")
    )
    password: Optional[str] = field(
        default_factory=lambda: os.getenv("CASSANDRA_PASSWORD")
    )

    @property
    def hosts_list(self) -> list:
        """Convert comma-separated hosts string to list."""
        return [h.strip() for h in self.hosts.split(",")]


@dataclass
class SparkConfig:
    """Spark configuration."""

    app_name: str = field(
        default_factory=lambda: os.getenv("SPARK_APP_NAME", "UserDataStreaming")
    )
    master: str = field(
        default_factory=lambda: os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    )
    log_level: str = field(default_factory=lambda: os.getenv("SPARK_LOG_LEVEL", "WARN"))
    checkpoint_location: str = field(
        default_factory=lambda: os.getenv(
            "SPARK_CHECKPOINT_LOCATION", "/tmp/checkpoint"
        )
    )
    packages: str = field(
        default_factory=lambda: os.getenv(
            "SPARK_PACKAGES",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
        )
    )


@dataclass
class APIConfig:
    """External API configuration."""

    base_url: str = field(
        default_factory=lambda: os.getenv("API_BASE_URL", "https://randomuser.me/api/")
    )
    timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("API_TIMEOUT_SECONDS", "30"))
    )
    max_retries: int = field(
        default_factory=lambda: int(os.getenv("API_MAX_RETRIES", "3"))
    )
    retry_base_delay: float = field(
        default_factory=lambda: float(os.getenv("API_RETRY_BASE_DELAY", "1.0"))
    )


@dataclass
class RetryConfig:
    """Retry behavior configuration."""

    max_retries: int = field(
        default_factory=lambda: int(os.getenv("RETRY_MAX_RETRIES", "3"))
    )
    base_delay: float = field(
        default_factory=lambda: float(os.getenv("RETRY_BASE_DELAY", "1.0"))
    )
    max_delay: float = field(
        default_factory=lambda: float(os.getenv("RETRY_MAX_DELAY", "60.0"))
    )
    exponential_base: float = field(
        default_factory=lambda: float(os.getenv("RETRY_EXPONENTIAL_BASE", "2.0"))
    )


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    format: str = field(
        default_factory=lambda: os.getenv(
            "LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    )
    json_format: bool = field(
        default_factory=lambda: os.getenv("LOG_JSON_FORMAT", "false").lower() == "true"
    )


@dataclass
class Settings:
    """Main settings container aggregating all component configs."""

    # Execution environment
    environment: Environment = field(
        default_factory=lambda: Environment(os.getenv("ENVIRONMENT", "development"))
    )

    # Component configs
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    cassandra: CassandraConfig = field(default_factory=CassandraConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    api: APIConfig = field(default_factory=APIConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    # Streaming behavior
    streaming_duration_seconds: int = field(
        default_factory=lambda: int(os.getenv("STREAMING_DURATION_SECONDS", "60"))
    )

    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == Environment.PRODUCTION

    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self.environment == Environment.DEVELOPMENT


# ============================================================
# Singleton Pattern
# ============================================================

_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the singleton Settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings() -> Settings:
    """Force reload settings (primarily for testing)."""
    global _settings
    _settings = Settings()
    return _settings
