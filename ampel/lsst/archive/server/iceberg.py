import functools
import json
import operator
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import datetime
from functools import cache
from logging import getLogger
from pathlib import Path
from tempfile import mktemp
from typing import Annotated
from urllib.parse import urljoin

import httpx
from duckdb import (
    ColumnExpression,
    DuckDBPyConnection,
    DuckDBPyRelation,
    Expression,
    SQLExpression,
    StarExpression,
    connect,
)
from duckdb.sqltypes import DuckDBPyType
from fastapi import Depends, HTTPException, Query, status
from pydantic import AfterValidator, BaseModel

from .settings import settings

log = getLogger(__name__)


@cache
def get_duckdb() -> DuckDBPyConnection:
    conn = connect()
    for ext in "httpfs", "iceberg":
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


def get_cursor(
    connection: Annotated[DuckDBPyConnection, Depends(get_duckdb)],
) -> Generator[DuckDBPyConnection, None, None]:
    cursor = connection.cursor()
    cursor.execute("use iceberg_catalog.lsst;")
    if settings.enable_profiling:
        profile_file = Path(mktemp(suffix=".json"))
        cursor.execute("set enable_profiling='json';")
        cursor.execute(f"set profile_output='{profile_file}';")
    try:
        yield cursor
    finally:
        if settings.enable_profiling and profile_file.exists():
            with profile_file.open() as f:
                profile = json.load(f)
            stripped = {
                k: v
                for k, v in profile.items()
                if k
                in {
                    "total_bytes_read",
                    "latency",
                    "cpu_time",
                    "system_peak_buffer_memory",
                }
            }
            log.warn(json.dumps(stripped, indent=2))
            profile_file.unlink()


Connection = Annotated[DuckDBPyConnection, Depends(get_cursor)]


async def get_refs():
    response = httpx.get(
        urljoin(str(settings.catalog_endpoint_url), "v1/namespaces/lsst/tables/alerts")
    )
    response.raise_for_status()
    metadata = response.json()["metadata"]
    refs = {snapshot["snapshot-id"]: snapshot for snapshot in metadata["snapshots"]}
    return [
        {
            "name": name,
            "type": ref["type"],
            "snapshot": refs.get(ref["snapshot-id"]),
        }
        for name, ref in metadata["refs"].items()
    ]


async def get_snapshot_id(
    branch: Annotated[str | None, Query()] = None,
    tag: Annotated[str | None, Query()] = None,
    timestamp: Annotated[datetime | None, Query()] = None,
) -> int | datetime | None:
    if timestamp is not None:
        return timestamp
    if tag is not None or branch is not None:
        response = httpx.get(
            urljoin(
                str(settings.catalog_endpoint_url), "v1/namespaces/lsst/tables/alerts"
            )
        )
        response.raise_for_status()
        metadata = response.json()
        ref = metadata["metadata"]["refs"].get(branch or tag)
        if ref is None:
            msg = f"Branch '{branch}' not found" if branch else f"Tag '{tag}' not found"
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail={"msg": msg}
            )
        return ref["snapshot-id"]
    return None


def get_relation(
    cursor: Connection,
    snapshot_id: Annotated[int | datetime | None, Depends(get_snapshot_id)] = None,
) -> DuckDBPyRelation:
    sql = "from iceberg_catalog.lsst.alerts"
    if isinstance(snapshot_id, int):
        sql += f" at (version => {snapshot_id})"
    elif isinstance(snapshot_id, datetime):
        sql += f" at (timestamp => timestamp '{snapshot_id.isoformat()}')"
    return cursor.sql(sql)


AlertRelation = Annotated[DuckDBPyRelation, Depends(get_relation)]


def flatten(relation: DuckDBPyRelation) -> list[dict]:
    return functools.reduce(
        operator.iconcat,
        (batch.to_pylist() for batch in relation.arrow()),
        [],
    )


def _names(name: str, dtype: DuckDBPyType) -> Generator[str, None, None]:
    yield name
    if dtype.id == "struct":
        for sub_name, sub_dtype in dtype.children:
            assert isinstance(sub_dtype, DuckDBPyType)
            yield from _names(f"{name}.{sub_name}", sub_dtype)


def _get_names(rel: DuckDBPyRelation) -> list[str]:
    names = []
    for name, dtype in zip(rel.columns, rel.types, strict=True):
        names.extend(_names(name, dtype))

    return names


@cache
def get_all_column_names() -> set[str]:
    with contextmanager(get_cursor)(get_duckdb()) as cursor:
        rel = get_relation(cursor)
        return set(_get_names(rel))


def is_valid_column(name: str) -> str:
    if name not in get_all_column_names():
        raise ValueError(f"Invalid column name: {name}")
    return name


Column = Annotated[str, AfterValidator(is_valid_column)]


class AlertQuery(BaseModel):
    include: list[Column] | None = None
    exclude: list[Column] | None = None
    condition: str
    limit: int | None = None
    order: str | None = None
    offset: int = 0

    def execute(
        self,
        relation: DuckDBPyRelation,
    ) -> DuckDBPyRelation:
        q = (
            relation
            if self.condition is None
            else relation.filter(SQLExpression(self.condition))
        ).select(*self.columns())
        if self.limit is not None:
            q = q.limit(self.limit, offset=self.offset)
        if self.order is not None:
            q = q.order(self.order)
        return q

    def flatten(self, relation: DuckDBPyRelation) -> list[dict]:
        return flatten(self.execute(relation))

    def columns(self) -> Sequence[Expression]:
        if self.include is None:
            return (StarExpression(exclude=self.exclude or []),)
        exclude_set = set(self.exclude or [])
        return [ColumnExpression(col) for col in self.include if col not in exclude_set]
