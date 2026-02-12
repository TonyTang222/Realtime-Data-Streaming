"""
Unit Tests for Resilient Cassandra Client
"""

import pytest
import time
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock, call

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.storage.cassandra_client import (
    ResilientCassandraClient,
    WriteResult,
    create_keyspace_and_table
)
from src.exceptions.custom_exceptions import (
    CassandraConnectionError,
    CassandraWriteError,
    CassandraReadError
)


@pytest.fixture
def mock_all_dependencies():
    """Mock all external dependencies for Cassandra client."""
    with patch('src.storage.cassandra_client.Cluster') as mock_cluster_class, \
         patch('src.storage.cassandra_client.get_settings') as mock_settings, \
         patch('src.storage.cassandra_client.get_logger') as mock_logger:

        # Setup mock cluster and session
        mock_cluster = MagicMock()
        mock_session = MagicMock()
        mock_cluster.connect.return_value = mock_session
        mock_cluster_class.return_value = mock_cluster

        # Setup mock settings
        mock_config = MagicMock()
        mock_config.cassandra.hosts = ['localhost']
        mock_config.cassandra.keyspace = 'test_keyspace'
        mock_settings.return_value = mock_config

        yield {
            'cluster_class': mock_cluster_class,
            'cluster': mock_cluster,
            'session': mock_session,
            'settings': mock_settings,
            'config': mock_config,
            'logger': mock_logger
        }


class TestResilientCassandraClient:
    """Unit tests for ResilientCassandraClient."""

    def test_connect_success(self, mock_all_dependencies):
        """Test successful connection."""
        mocks = mock_all_dependencies
        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')

        session = client.connect()

        assert session is mocks['session']
        assert client._session is mocks['session']
        mocks['cluster'].connect.assert_called_once_with('test_keyspace')

    def test_connect_failure_retries(self, mock_all_dependencies):
        """Test connection retries on failure."""
        from cassandra.cluster import NoHostAvailable

        mocks = mock_all_dependencies
        mocks['cluster'].connect.side_effect = NoHostAvailable("No hosts available", {})

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')

        with pytest.raises(NoHostAvailable):
            client.connect()

        # Should have retried multiple times
        assert mocks['cluster'].connect.call_count >= 1

    def test_session_lazy_property(self, mock_all_dependencies):
        """Test session property lazy initialization."""
        mocks = mock_all_dependencies
        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')

        assert client._session is None
        session = client.session
        assert session is mocks['session']

    # ============================================================
    # Execute Tests
    # ============================================================

    def test_execute_simple_query(self, mock_all_dependencies):
        """Test executing a simple query."""
        mocks = mock_all_dependencies
        mock_result = MagicMock()
        mocks['session'].execute.return_value = mock_result

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']

        result = client.execute("SELECT * FROM users")

        assert result is mock_result
        mocks['session'].execute.assert_called_once()

    def test_execute_with_parameters_uses_prepared_statement(self, mock_all_dependencies):
        """Test query with parameters uses prepared statement."""
        mocks = mock_all_dependencies
        mock_prepared = MagicMock()
        mock_bound = MagicMock()
        mock_prepared.bind.return_value = mock_bound
        mocks['session'].prepare.return_value = mock_prepared
        mocks['session'].execute.return_value = MagicMock()

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']

        client.execute("SELECT * FROM users WHERE id = ?", ("uuid-123",))

        mocks['session'].prepare.assert_called_once()
        mock_prepared.bind.assert_called_once_with(("uuid-123",))
        mocks['session'].execute.assert_called_once_with(mock_bound)

    def test_execute_caches_prepared_statements(self, mock_all_dependencies):
        """Test prepared statements are cached."""
        mocks = mock_all_dependencies
        mock_prepared = MagicMock()
        mock_prepared.bind.return_value = MagicMock()
        mocks['session'].prepare.return_value = mock_prepared
        mocks['session'].execute.return_value = MagicMock()

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']
        query = "SELECT * FROM users WHERE id = ?"

        client.execute(query, ("uuid-1",))
        client.execute(query, ("uuid-2",))
        client.execute(query, ("uuid-3",))

        # prepare should only be called once
        mocks['session'].prepare.assert_called_once_with(query)
        assert mocks['session'].execute.call_count == 3

    def test_execute_write_failure_raises_write_error(self, mock_all_dependencies):
        """Test write failure raises CassandraWriteError."""
        mocks = mock_all_dependencies
        mock_prepared = MagicMock()
        mock_prepared.bind.return_value = MagicMock()
        mocks['session'].prepare.return_value = mock_prepared
        mocks['session'].execute.side_effect = Exception("Write failed")

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']

        with pytest.raises(CassandraWriteError):
            client.execute("INSERT INTO users (id, name) VALUES (?, ?)", ("uuid", "test"))

    def test_execute_read_failure_raises_read_error(self, mock_all_dependencies):
        """Test read failure raises CassandraReadError."""
        mocks = mock_all_dependencies
        mocks['session'].execute.side_effect = Exception("Read failed")

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']

        with pytest.raises(CassandraReadError):
            client.execute("SELECT * FROM users")

    # ============================================================
    # Batch Execution Tests
    # ============================================================

    def test_execute_batch_success(self, mock_all_dependencies):
        """Test batch execution succeeds."""
        mocks = mock_all_dependencies
        mock_prepared = MagicMock()
        mocks['session'].prepare.return_value = mock_prepared
        mocks['session'].execute.return_value = MagicMock()

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']
        queries = [
            ("INSERT INTO users (id, name) VALUES (?, ?)", ("uuid-1", "Alice")),
            ("INSERT INTO users (id, name) VALUES (?, ?)", ("uuid-2", "Bob")),
        ]

        result = client.execute_batch(queries)

        assert isinstance(result, WriteResult)
        assert result.success is True
        assert result.rows_affected == 2
        assert result.error is None

    def test_execute_batch_partial_failure(self, mock_all_dependencies):
        """Test batch partial failure."""
        mocks = mock_all_dependencies
        mock_prepared = MagicMock()
        mocks['session'].prepare.return_value = mock_prepared

        # First batch succeeds, second batch fails
        mocks['session'].execute.side_effect = [MagicMock(), Exception("Batch failed")]

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']
        queries = [("INSERT INTO users (id) VALUES (?)", (f"uuid-{i}",)) for i in range(150)]

        result = client.execute_batch(queries, batch_size=100)

        assert result.success is False
        assert result.rows_affected == 100  # First batch succeeded
        assert result.error is not None

    def test_execute_batch_respects_batch_size(self, mock_all_dependencies):
        """Test batch size limit is respected."""
        mocks = mock_all_dependencies
        mock_prepared = MagicMock()
        mocks['session'].prepare.return_value = mock_prepared
        mocks['session'].execute.return_value = MagicMock()

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']
        queries = [("INSERT INTO users (id) VALUES (?)", (f"uuid-{i}",)) for i in range(250)]

        client.execute_batch(queries, batch_size=100)

        # Should have 3 batch executions (100 + 100 + 50)
        assert mocks['session'].execute.call_count == 3

    # ============================================================
    # Insert User Tests
    # ============================================================

    def test_insert_user_success(self, mock_all_dependencies, sample_transformed_user):
        """Test inserting a user succeeds."""
        mocks = mock_all_dependencies
        mock_prepared = MagicMock()
        mock_prepared.bind.return_value = MagicMock()
        mocks['session'].prepare.return_value = mock_prepared
        mocks['session'].execute.return_value = MagicMock()

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']
        result = client.insert_user(sample_transformed_user)

        assert result is True

    def test_insert_user_failure(self, mock_all_dependencies, sample_transformed_user):
        """Test inserting a user fails gracefully."""
        mocks = mock_all_dependencies
        mock_prepared = MagicMock()
        mock_prepared.bind.return_value = MagicMock()
        mocks['session'].prepare.return_value = mock_prepared
        mocks['session'].execute.side_effect = Exception("Insert failed")

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']
        result = client.insert_user(sample_transformed_user)

        assert result is False

    # ============================================================
    # Health Check Tests
    # ============================================================

    def test_health_check_success(self, mock_all_dependencies):
        """Test health check succeeds."""
        mocks = mock_all_dependencies
        mocks['session'].execute.return_value = MagicMock()

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']
        result = client.health_check()

        assert result is True

    def test_health_check_failure(self, mock_all_dependencies):
        """Test health check fails."""
        mocks = mock_all_dependencies
        mocks['session'].execute.side_effect = Exception("Connection lost")

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._session = mocks['session']
        result = client.health_check()

        assert result is False

    # ============================================================
    # Resource Cleanup Tests
    # ============================================================

    def test_close_shuts_down_cluster_and_session(self, mock_all_dependencies):
        """Test close() shuts down cluster and session."""
        mocks = mock_all_dependencies

        client = ResilientCassandraClient(hosts=['localhost'], keyspace='test_keyspace')
        client._cluster = mocks['cluster']
        client._session = mocks['session']
        client._prepared_statements = {"query": MagicMock()}

        client.close()

        mocks['session'].shutdown.assert_called_once()
        mocks['cluster'].shutdown.assert_called_once()
        assert client._session is None
        assert client._cluster is None
        assert len(client._prepared_statements) == 0

    def test_context_manager_connects_and_closes(self, mock_all_dependencies):
        """Test context manager connects and closes properly."""
        mocks = mock_all_dependencies

        with ResilientCassandraClient(hosts=['localhost'], keyspace='test') as client:
            assert client._session is not None

        mocks['session'].shutdown.assert_called_once()
        mocks['cluster'].shutdown.assert_called_once()


class TestWriteResult:
    """Tests for WriteResult dataclass."""

    def test_write_result_defaults(self):
        """Test WriteResult default values."""
        result = WriteResult(success=True)

        assert result.success is True
        assert result.rows_affected == 0
        assert result.latency_ms == 0.0
        assert result.error is None

    def test_write_result_with_values(self):
        """Test WriteResult with custom values."""
        result = WriteResult(
            success=False,
            rows_affected=50,
            latency_ms=123.45,
            error="Some error"
        )

        assert result.success is False
        assert result.rows_affected == 50
        assert result.latency_ms == 123.45
        assert result.error == "Some error"


class TestCassandraHelperFunctions:
    """Tests for Cassandra helper functions."""

    def test_create_keyspace_and_table(self, mock_all_dependencies):
        """Test create_keyspace_and_table executes required statements."""
        mocks = mock_all_dependencies

        create_keyspace_and_table(keyspace='test_ks', replication_factor=1)

        # Should execute CREATE KEYSPACE, USE, and CREATE TABLE
        assert mocks['session'].execute.call_count >= 3
