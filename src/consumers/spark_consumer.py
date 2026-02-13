"""Spark Streaming Consumer"""

import logging
from typing import Optional

from cassandra.cluster import Cluster
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json

from ..config.schemas import (
    CASSANDRA_CREATE_KEYSPACE_CQL,
    CASSANDRA_CREATE_TABLE_CQL,
    SPARK_USER_SCHEMA,
)
from ..config.settings import Settings, get_settings
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class SparkStreamingConsumer:
    """
    Spark Streaming Consumer.

    Usage::

        consumer = SparkStreamingConsumer()
        consumer.run()
    """

    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialize Spark Streaming Consumer.

        Args:
            settings: Configuration object, defaults to get_settings().
        """
        self.settings = settings or get_settings()
        self._spark: Optional[SparkSession] = None
        self._cassandra_session = None

        logger.info(
            "SparkStreamingConsumer initialized",
            extra={
                "kafka_servers": self.settings.kafka.bootstrap_servers,
                "kafka_topic": self.settings.kafka.topic_user_data,
                "cassandra_keyspace": self.settings.cassandra.keyspace,
            },
        )

    def create_spark_connection(self) -> Optional[SparkSession]:
        """
        Create a Spark Session.

        Returns:
            SparkSession, or None if creation fails.
        """
        if self._spark is not None:
            return self._spark

        try:
            self._spark = (
                SparkSession.builder.appName(self.settings.spark.app_name)
                .config("spark.jars.packages", self.settings.spark.packages)
                .config(
                    "spark.cassandra.connection.host", self.settings.cassandra.hosts
                )
                .config(
                    "spark.cassandra.connection.port", str(self.settings.cassandra.port)
                )
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel(self.settings.spark.log_level)
            logger.info("Spark session created successfully")
            return self._spark

        except Exception as e:
            logger.error(f"Cannot create Spark session: {e}")
            return None

    @property
    def spark(self) -> Optional[SparkSession]:
        """Lazy initialization of Spark session."""
        if self._spark is None:
            self._spark = self.create_spark_connection()
        return self._spark

    def connect_to_kafka(self) -> Optional[DataFrame]:
        """
        Connect to Kafka and create a streaming DataFrame.

        Returns:
            Spark DataFrame, or None if connection fails.
        """
        if self.spark is None:
            logger.error("No Spark session available")
            return None

        try:
            spark_df = (
                self.spark.readStream.format("kafka")
                .option(
                    "kafka.bootstrap.servers", self.settings.kafka.bootstrap_servers
                )
                .option("subscribe", self.settings.kafka.topic_user_data)
                .option("startingOffsets", "earliest")
                .load()
            )
            logger.info("Kafka DataFrame created successfully")
            return spark_df

        except Exception as e:
            logger.warning(f"Kafka DataFrame could not be created: {e}")
            return None

    def create_selection_df_from_kafka(self, spark_df: DataFrame) -> DataFrame:
        """
        Parse JSON from a Kafka DataFrame and select user fields.

        Args:
            spark_df: Raw Kafka DataFrame.

        Returns:
            Parsed DataFrame with all user fields.
        """
        selection_df = (
            spark_df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), SPARK_USER_SCHEMA).alias("data"))
            .select("data.*")
        )

        logger.debug("Selection DataFrame created from Kafka stream")
        return selection_df

    def create_cassandra_connection(self):
        """
        Create a Cassandra connection.

        Returns:
            Cassandra session, or None if connection fails.
        """
        if self._cassandra_session is not None:
            return self._cassandra_session

        try:
            cluster = Cluster(
                self.settings.cassandra.hosts_list, port=self.settings.cassandra.port
            )
            self._cassandra_session = cluster.connect()
            logger.info("Cassandra connection established")
            return self._cassandra_session

        except Exception as e:
            logger.error(f"Cannot create Cassandra connection: {e}")
            return None

    def create_keyspace(self, session) -> None:
        """
        Create the Cassandra keyspace.

        Args:
            session: Cassandra session.
        """
        cql = CASSANDRA_CREATE_KEYSPACE_CQL.format(
            keyspace=self.settings.cassandra.keyspace,
            replication_factor=self.settings.cassandra.replication_factor,
        )
        session.execute(cql)
        logger.info(f"Keyspace '{self.settings.cassandra.keyspace}' created/verified")

    def create_table(self, session) -> None:
        """
        Create the Cassandra table.

        Args:
            session: Cassandra session.
        """
        cql = CASSANDRA_CREATE_TABLE_CQL.format(
            keyspace=self.settings.cassandra.keyspace,
            table=self.settings.cassandra.table,
        )
        session.execute(cql)
        logger.info(f"Table '{self.settings.cassandra.table}' created/verified")

    def setup_cassandra(self) -> bool:
        """
        Set up Cassandra (create keyspace and table).

        Returns:
            True if setup succeeds, False otherwise.
        """
        session = self.create_cassandra_connection()
        if session is None:
            return False

        try:
            self.create_keyspace(session)
            self.create_table(session)
            return True
        except Exception as e:
            logger.error(f"Failed to setup Cassandra: {e}")
            return False

    def start_streaming(self, selection_df: DataFrame) -> None:
        """
        Start streaming writes to Cassandra.

        Args:
            selection_df: DataFrame to write.
        """
        logger.info("Starting streaming to Cassandra...")

        streaming_query = (
            selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", self.settings.spark.checkpoint_location)
            .option("keyspace", self.settings.cassandra.keyspace)
            .option("table", self.settings.cassandra.table)
            .start()
        )

        logger.info("Streaming started, awaiting termination...")
        streaming_query.awaitTermination()

    def run(self) -> None:
        """Run the full streaming pipeline."""
        logger.info("Starting Spark Streaming Consumer...")

        if self.spark is None:
            logger.error("Failed to create Spark session, exiting")
            return

        kafka_df = self.connect_to_kafka()
        if kafka_df is None:
            logger.error("Failed to connect to Kafka, exiting")
            return

        selection_df = self.create_selection_df_from_kafka(kafka_df)

        if not self.setup_cassandra():
            logger.error("Failed to setup Cassandra, exiting")
            return

        self.start_streaming(selection_df)

    def close(self) -> None:
        """Close all connections."""
        if self._cassandra_session:
            self._cassandra_session.shutdown()
            self._cassandra_session = None
            logger.info("Cassandra session closed")

        if self._spark:
            self._spark.stop()
            self._spark = None
            logger.info("Spark session stopped")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def main():
    """CLI entry point."""
    logging.basicConfig(level=logging.INFO)

    with SparkStreamingConsumer() as consumer:
        consumer.run()


if __name__ == "__main__":
    main()
