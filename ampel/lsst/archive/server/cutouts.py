from typing import Any, BinaryIO

import fastavro

ALERT_SCHEMAS: dict[str, Any] = {}


def get_parsed_schema(schema_id: int, schema: dict):
    key = schema_id
    if key not in ALERT_SCHEMAS:
        ALERT_SCHEMAS[key] = fastavro.parse_schema(schema)
    return ALERT_SCHEMAS[key]


def read_schema(fo: BinaryIO) -> dict[str, Any]:
    reader = fastavro.reader(fo)
    assert isinstance(reader.writer_schema, dict)
    return reader.writer_schema
