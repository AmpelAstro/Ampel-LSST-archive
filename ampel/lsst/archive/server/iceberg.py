from functools import cache
from typing import Annotated

from duckdb import DuckDBPyConnection, connect
from fastapi import Depends

from .settings import settings


@cache
def get_duckdb() -> DuckDBPyConnection:
    conn = connect()
    for ext in "httpfs", "iceberg":
        conn.install_extension(ext)
        conn.load_extension(ext)
    conn.execute(f"""
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER config,
            REGION 'us-east-1',
            ENDPOINT '{settings.s3_endpoint}',
            URL_STYLE 'path'
        );
        ATTACH 'warehouse' AS iceberg_catalog(
            TYPE iceberg, AUTHORIZATION_TYPE none,
            ENDPOINT '{settings.catalog_endpoint_url}'
        );
    """)
    return conn


def get_cursor() -> DuckDBPyConnection:
    return get_duckdb().execute("use iceberg_catalog.lsst;")


Connection = Annotated[DuckDBPyConnection, Depends(get_cursor)]
