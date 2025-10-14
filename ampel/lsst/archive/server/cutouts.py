from typing import Any, BinaryIO

import fastavro
from fastavro._read_py import BLOCK_READERS, BinaryDecoder

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


def extract_alert(
    candid: int, block: BinaryIO, schema: dict, codec: str = "null"
) -> dict[str, Any]:
    """
    Extract single record from Avro block
    """
    read_block = BLOCK_READERS.get(codec)
    if read_block is None:
        raise KeyError(f"Unknown codec {codec}")

    decoder = BinaryDecoder(block)
    # consume record count to advance to the compressed block
    nrecords = decoder.read_long()
    assert nrecords > 0
    # consume compressed block
    buf = read_block(decoder)
    # iterate over deserialized records
    for _ in range(nrecords):
        alert = fastavro.schemaless_reader(buf, get_parsed_schema(schema), None)
        if isinstance(alert, dict) and alert["candid"] == candid:
            return alert
    raise KeyError(f"{candid} not found in block")
