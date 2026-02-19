import os
import sys
from pathlib import Path

import duckdb
import httpx
import pytest
from fastapi import status
from pyiceberg.catalog import load_catalog
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy, LogMessageWaitStrategy

from ampel.lsst.archive.server.iceberg import get_duckdb
from ampel.lsst.archive.server.settings import settings


@pytest.fixture(scope="session")
def _docker(pytestconfig):
    if not pytestconfig.getoption("--integration"):
        pytest.skip("Integration tests are disabled. Use --integration to enable them.")
    rd_socket = Path.home() / ".rd/docker.sock"
    if rd_socket.exists():
        os.environ["DOCKER_HOST"] = f"unix://{rd_socket}"
        os.environ["TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"] = "/var/run/docker.sock"


@pytest.fixture(scope="session")
def warehouse_dir(tmp_path_factory) -> Path:
    """Fixture to create a temporary directory for the warehouse."""
    tmp: Path = tmp_path_factory.mktemp("warehouse")
    tmp.chmod(0o777)  # make it writable by any user (e.g., in Docker)
    return tmp


@pytest.fixture(scope="session")
def catalog(_docker, warehouse_dir: Path):
    """Start an Iceberg REST catalog server in a Docker container and yield its URL."""
    port = 8181
    with (
        DockerContainer(image="apache/iceberg-rest-fixture:1.10.0")
        .with_exposed_ports(port)
        .with_volume_mapping(warehouse_dir, str(warehouse_dir.as_posix()), mode="rw")
        .with_env("CATALOG_WAREHOUSE", warehouse_dir)
        .waiting_for(LogMessageWaitStrategy("Started Server").with_startup_timeout(10))
        .waiting_for(
            HttpWaitStrategy(port).for_status_code(400).with_startup_timeout(10)
        )
    ) as container:
        url = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(port)}"
        try:
            assert (
                httpx.get(
                    f"{url}/v1/namespaces",
                    timeout=1,
                ).status_code
                == status.HTTP_200_OK
            )
        except:
            out, err = container.get_logs()
            sys.stdout.buffer.write(out)
            sys.stdout.flush()
            sys.stderr.buffer.write(err)
            sys.stderr.flush()
            raise

        yield url

        out, err = container.get_logs()
        sys.stdout.buffer.write(out)
        sys.stdout.flush()
        sys.stderr.buffer.write(err)
        sys.stderr.flush()


@pytest.fixture(scope="session")
def _alert_table(catalog, warehouse_dir: Path):
    """Fixture to create a DuckDB connection to the Iceberg catalog."""

    # duckdb fileio expects directory structure to exist when creating tables
    table_dir = warehouse_dir / "lsst" / "alerts"
    table_dir.mkdir(parents=True)
    for subdir in ["data", "metadata"]:
        p = table_dir / subdir
        p.mkdir()  # "On some systems, mode is ignored"
        p.chmod(0o777)

    cursor = duckdb.connect()
    cursor.execute(f"""
        ATTACH 'warehouse' AS iceberg_catalog(
            TYPE iceberg, AUTHORIZATION_TYPE none,
            ENDPOINT '{catalog}'
        );
    """)
    cursor.execute("create schema iceberg_catalog.lsst;")
    cursor.execute("use iceberg_catalog.lsst;")

    cursor.execute(
        """
        create table iceberg_catalog.lsst.alerts
        as (
            select * from read_parquet('tests/test-data/diaObject.313853499427782700.parquet') limit 10
        );
        """
    )

    # create a second snapshot
    cursor.execute(
        """
        insert into iceberg_catalog.lsst.alerts (
            select * from read_parquet('tests/test-data/diaObject.313853499427782700.parquet') offset 10
        );
        """
    )

    return cursor


@pytest.fixture(scope="session")
def pyiceberg_catalog(catalog, warehouse_dir: Path):
    """Fixture to create a PyIceberg connection to the Iceberg catalog."""

    return load_catalog(
        "iceberg",
        uri=catalog,
        warehouse=str(warehouse_dir.as_posix()),
    )


@pytest.fixture(scope="session")
def alert_table_branch(pyiceberg_catalog, _alert_table):
    """Fixture to create a table with multiple branches."""

    table = pyiceberg_catalog.load_table("lsst.alerts")
    snapshots = list(table.snapshots())
    assert len(table.snapshots()) == 2, "Expected 2 snapshots in the table"
    branch_name = "test_branch"
    with table.manage_snapshots() as ctx:
        ctx.create_branch(snapshots[0].snapshot_id, branch_name)
    return branch_name


@pytest.fixture
def _mock_iceberg(catalog, _alert_table, monkeypatch):
    monkeypatch.setattr(settings, "catalog_endpoint_url", catalog)

    get_duckdb.cache_clear()
    yield
    get_duckdb.cache_clear()
