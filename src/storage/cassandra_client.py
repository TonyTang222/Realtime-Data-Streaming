"""Resilient Cassandra Client."""

import time
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

from cassandra.cluster import Cluster, Session, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    ExponentialReconnectionPolicy
)

from ..config.settings import get_settings
from ..utils.logging_config import get_logger
from ..utils.retry import exponential_backoff_retry
from ..exceptions.custom_exceptions import (
    CassandraConnectionError,
    CassandraWriteError,
    CassandraReadError
)

logger = get_logger(__name__)


@dataclass
class WriteResult:
    """Result of a write operation."""
    success: bool
    rows_affected: int = 0
    latency_ms: float = 0.0
    error: Optional[str] = None


class ResilientCassandraClient:
    """Cassandra client with automatic retry, connection pooling, and prepared statement caching.

    Usage::

        with ResilientCassandraClient() as client:
            client.execute("INSERT INTO users (id, name) VALUES (?, ?)", [uuid, name])
    """

    def __init__(
        self,
        hosts: Optional[List[str]] = None,
        keyspace: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        consistency_level: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM
    ):
        """Initialize Cassandra client.

        Args:
            hosts: Cassandra node list (defaults from settings).
            keyspace: Default keyspace.
            username: Authentication username.
            password: Authentication password.
            consistency_level: Query consistency level.
        """
        self.settings = get_settings()

        self.hosts = hosts or self.settings.cassandra.hosts
        self.keyspace = keyspace or self.settings.cassandra.keyspace
        self.username = username
        self.password = password
        self.consistency_level = consistency_level

        self._cluster: Optional[Cluster] = None
        self._session: Optional[Session] = None
        self._prepared_statements: Dict[str, Any] = {}

        logger.info(
            "CassandraClient initialized",
            extra={
                'hosts': self.hosts,
                'keyspace': self.keyspace,
                'consistency_level': str(consistency_level)
            }
        )

    def _create_cluster(self) -> Cluster:
        """Create a Cassandra Cluster instance with load balancing and reconnection policies."""
        auth_provider = None
        if self.username and self.password:
            auth_provider = PlainTextAuthProvider(
                username=self.username,
                password=self.password
            )

        cluster = Cluster(
            contact_points=self.hosts,
            auth_provider=auth_provider,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
            reconnection_policy=ExponentialReconnectionPolicy(
                base_delay=1.0,
                max_delay=60.0
            ),
            protocol_version=4
        )

        return cluster

    @exponential_backoff_retry(
        max_retries=5,
        base_delay=1.0,
        retryable_exceptions=(NoHostAvailable,)
    )
    def connect(self) -> Session:
        """Connect to Cassandra with exponential backoff retry.

        Returns:
            Cassandra Session.

        Raises:
            CassandraConnectionError: If connection fails after all retries.
        """
        try:
            logger.info(f"Connecting to Cassandra: {self.hosts}")
            start_time = time.time()

            self._cluster = self._create_cluster()
            self._session = self._cluster.connect(self.keyspace)

            latency = (time.time() - start_time) * 1000
            logger.info(
                "Connected to Cassandra",
                extra={'latency_ms': latency}
            )
            logger.debug("cassandra_connect_latency_ms: %.2f", latency)

            return self._session

        except NoHostAvailable as e:
            logger.error(f"No Cassandra host available: {e}")
            logger.debug("cassandra_connection_failures incremented")
            raise

        except Exception as e:
            logger.error(f"Cassandra connection error: {e}")
            logger.debug("cassandra_connection_errors incremented")
            raise CassandraConnectionError(f"Failed to connect: {e}")

    @property
    def session(self) -> Session:
        """Get session with lazy initialization."""
        if self._session is None:
            self.connect()
        return self._session

    def prepare(self, query: str):
        """Prepare and cache a CQL statement.

        Args:
            query: CQL query string.

        Returns:
            PreparedStatement.
        """
        if query not in self._prepared_statements:
            self._prepared_statements[query] = self.session.prepare(query)
            logger.debug(f"Prepared statement cached: {query[:50]}...")

        return self._prepared_statements[query]

    def execute(
        self,
        query: str,
        parameters: Optional[tuple] = None,
        consistency_level: Optional[ConsistencyLevel] = None
    ) -> Any:
        """Execute a CQL query.

        Args:
            query: CQL query or prepared statement.
            parameters: Query parameters.
            consistency_level: Override the default consistency level.

        Returns:
            Query result.

        Raises:
            CassandraWriteError: If a write operation fails.
            CassandraReadError: If a read operation fails.
        """
        start_time = time.time()
        level = consistency_level or self.consistency_level

        try:
            if isinstance(query, str) and parameters:
                prepared = self.prepare(query)
                statement = prepared.bind(parameters)
            else:
                statement = SimpleStatement(query, consistency_level=level)

            result = self.session.execute(statement)

            latency = (time.time() - start_time) * 1000
            logger.debug(
                "Query executed",
                extra={'latency_ms': latency, 'query': query[:50]}
            )

            return result

        except Exception as e:
            latency = (time.time() - start_time) * 1000
            logger.error(
                f"Query failed: {e}",
                extra={'query': query[:50], 'latency_ms': latency}
            )

            query_upper = query.strip().upper()
            if query_upper.startswith(('INSERT', 'UPDATE', 'DELETE')):
                raise CassandraWriteError(f"Write failed: {e}")
            else:
                raise CassandraReadError(f"Read failed: {e}")

    def execute_batch(
        self,
        queries: List[tuple],
        batch_size: int = 100
    ) -> WriteResult:
        """Execute multiple writes in batches.

        Args:
            queries: List of (query, parameters) tuples.
            batch_size: Maximum statements per batch.

        Returns:
            WriteResult.
        """
        from cassandra.query import BatchStatement, BatchType

        start_time = time.time()
        total_rows = 0
        errors = []

        for i in range(0, len(queries), batch_size):
            batch_queries = queries[i:i + batch_size]

            try:
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)

                for query, params in batch_queries:
                    prepared = self.prepare(query)
                    batch.add(prepared, params)

                self.session.execute(batch)
                total_rows += len(batch_queries)

            except Exception as e:
                errors.append(str(e))
                logger.error(f"Batch execution failed: {e}")

        latency = (time.time() - start_time) * 1000

        result = WriteResult(
            success=len(errors) == 0,
            rows_affected=total_rows,
            latency_ms=latency,
            error='; '.join(errors) if errors else None
        )

        logger.info(
            "Batch execution complete",
            extra={
                'rows_affected': total_rows,
                'errors': len(errors),
                'latency_ms': latency
            }
        )

        return result

    def insert_user(self, user_data: Dict[str, Any]) -> bool:
        """Insert a user record.

        Args:
            user_data: Transformed user data dict.

        Returns:
            True on success, False on failure.
        """
        query = """
            INSERT INTO created_users (
                id, first_name, last_name, gender, address,
                email, username, dob, registered_date, phone, picture
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        try:
            self.execute(query, (
                user_data['id'],
                user_data['first_name'],
                user_data['last_name'],
                user_data['gender'],
                user_data['address'],
                user_data['email'],
                user_data['username'],
                user_data.get('dob', ''),
                user_data['registered_date'],
                user_data['phone'],
                user_data['picture']
            ))

            logger.debug("User inserted successfully")
            return True

        except CassandraWriteError:
            logger.warning("User insert failed")
            return False

    def health_check(self) -> bool:
        """Check whether Cassandra is reachable."""
        try:
            self.execute("SELECT now() FROM system.local")
            return True
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

    def close(self):
        """Close the connection."""
        if self._session:
            self._session.shutdown()
            self._session = None

        if self._cluster:
            self._cluster.shutdown()
            self._cluster = None

        self._prepared_statements.clear()
        logger.info("Cassandra connection closed")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def create_keyspace_and_table(
    keyspace: str = 'spark_streams',
    replication_factor: int = 1
) -> None:
    """Create keyspace and table for initial setup.

    Args:
        keyspace: Keyspace name.
        replication_factor: Replication factor.
    """
    client = ResilientCassandraClient(keyspace=None)

    try:
        client.connect()

        client.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': {replication_factor}
            }}
        """)
        logger.info(f"Keyspace '{keyspace}' created/verified")

        client.execute(f"USE {keyspace}")

        client.execute("""
            CREATE TABLE IF NOT EXISTS created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                email TEXT,
                username TEXT,
                dob TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT
            )
        """)
        logger.info("Table 'created_users' created/verified")

    finally:
        client.close()


