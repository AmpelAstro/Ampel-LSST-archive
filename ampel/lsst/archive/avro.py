import io
from collections.abc import Iterable
from os import urandom
from typing import TYPE_CHECKING, Any, BinaryIO

import fastavro
from fastavro import writer
from fastavro._read_common import SYNC_SIZE
from fastavro._read_py import BLOCK_READERS
from fastavro.io.binary_decoder import BinaryDecoder

if TYPE_CHECKING:
    from fastavro.types import AvroMessage, Schema


def extract_record(
    block: BinaryIO, schema: "Schema", codec: str = "zstandard"
) -> "AvroMessage":
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
    schema: "Schema", records: Iterable[Any], codec: str = "zstandard"
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


def pack_blocks(
    schema: "Schema", blocks: Iterable[bytes], codec: str = "zstandard"
) -> bytes:
    """
    pack individual blocks (possibly from different sources) into a single Avro file
    """
    # generate a new sync marker
    sync_marker = urandom(SYNC_SIZE)
    with io.BytesIO() as buf:
        writer(
            buf,
            schema,
            [],
            codec=codec,
            sync_marker=sync_marker,
        )
        for block in blocks:
            buf.write(block[: -SYNC_SIZE - 1])  # skip sync marker
            buf.write(sync_marker)  # write our own sync marker
        return buf.getvalue()
