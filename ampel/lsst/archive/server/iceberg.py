import functools
import operator
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from functools import cache
from typing import Annotated

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
from fastapi import Depends
from pydantic import AfterValidator, BaseModel

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
    conn.execute("use iceberg_catalog.lsst;")
    return conn


def get_cursor(
    connection: Annotated[DuckDBPyConnection, Depends(get_duckdb)],
) -> Generator[DuckDBPyConnection, None, None]:
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


Connection = Annotated[DuckDBPyConnection, Depends(get_cursor)]


def get_relation(cursor: Connection) -> DuckDBPyRelation:
    return cursor.sql("from iceberg_catalog.lsst.alerts")


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
