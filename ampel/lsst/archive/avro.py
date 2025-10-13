
import io
from collections.abc import Iterable
from typing import Any, BinaryIO

import fastavro
from fastavro._read_py import BLOCK_READERS, BinaryDecoder
from fastavro._write_py import writer


def extract_record(
    block: BinaryIO, schema: dict, codec: str = "zstandard"
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
    assert nrecords == 1
    # consume compressed block
    buf = read_block(decoder)
    # deserialized record
    return fastavro.schemaless_reader(buf, schema, None)


def pack_records(
    schema: dict, records: Iterable[dict], codec: str = "zstandard"
) -> tuple[bytes, list[tuple[int, int]]]:
    
    # reserialize into schemafull format with one record per block
    with io.BytesIO() as buf:
        writer(
            buf,
            schema,
            records,
            codec=codec,
            sync_interval=0,  # force one record per block
        )
        packed = buf.getvalue()

    # read back to extract the offsets of each block
    with io.BytesIO(packed) as buf:
        reader = fastavro.block_reader(buf)
        ranges = []
        start = buf.tell()
        for block in reader:
            end = buf.tell()
            if len(list(block)) != 1:
                raise ValueError("Expected single record per block")
            ranges.append((start, end))
            start = end

    return packed, ranges

