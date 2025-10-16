import logging
import signal
import struct
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from threading import Event
from typing import Annotated

import typer
from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from ampel.lsst.archive.db import ensure_schema, insert_alert_chunk
from ampel.lsst.archive.server.db import get_engine
from ampel.lsst.archive.server.s3 import get_s3_bucket


def _raise_errors(exc: Exception) -> None:
    raise exc


log = logging.getLogger(__name__)


class AvroWithSchemaDeserializer:
    def __init__(self, schema_registry_client: SchemaRegistryClient):
        self.schema_registry_client = schema_registry_client
        self.inner_deserializer = AvroDeserializer(self.schema_registry_client)
        self._schema_cache: dict[int, str] = {}

    def __call__(self, message: Message) -> tuple[int, str, dict]:
        ctx = SerializationContext(message.topic(), MessageField.VALUE)
        payload = message.value()
        assert isinstance(payload, bytes)
        record = self.inner_deserializer(payload, ctx)
        _, schema_id = struct.unpack(">bI", payload[:5])

        if schema_id not in self._schema_cache:
            schema = self.schema_registry_client.get_schema(schema_id)
            assert isinstance(schema.schema_str, str)
            self._schema_cache[schema_id] = schema.schema_str

        return (schema_id, self._schema_cache[schema_id], record)


@dataclass
class PartitionBuffer:
    records: list[dict]
    schema_id: int
    schema_str: str
    start_offset: TopicPartition
    end_offset: TopicPartition
    last_seen: datetime


def main(
    username: Annotated[str, typer.Option(envvar="KAFKA_USERNAME")],
    password: Annotated[str, typer.Option(envvar="KAFKA_PASSWORD")],
    broker: Annotated[
        str, typer.Option(envvar="KAFKA_BROKER")
    ] = "usdf-alert-stream-dev.lsst.cloud:9094",
    registry: Annotated[
        str, typer.Option(envvar="KAFKA_SCHEMA_REGISTRY")
    ] = "https://usdf-alert-schemas-dev.slac.stanford.edu",
    topic: Annotated[str, typer.Option(envvar="KAFKA_TOPIC")] = "lsst-alerts-v9.0",
    group: Annotated[str, typer.Option(envvar="KAFKA_GROUP")] = "ampel-idfint-archive",
    instance: Annotated[None | str, typer.Option(envvar="HOSTNAME")] = None,
    store_offsets: Annotated[bool, typer.Option()] = True,
    chunk_size: Annotated[int, typer.Option()] = 1000,
    timeout: Annotated[float, typer.Option()] = 300.0,
):
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group,
            "group.instance.id": instance,
            "partition.assignment.strategy": "cooperative-sticky",
            "auto.offset.reset": "earliest",
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": username,
            "sasl.password": password,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": False,
            "error_cb": _raise_errors,
        }
    )
    unpack = AvroWithSchemaDeserializer(SchemaRegistryClient({"url": registry}))

    buffers: dict[tuple[str, int], PartitionBuffer] = {}

    def on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """
        Drop any buffered records for revoked partitions.
        """
        log.info(f"Partitions revoked: {partitions}")
        dropped = [
            buffers.pop(k)
            for tp in partitions
            if (k := (tp.topic, tp.partition)) in buffers
        ]
        log.info(
            f"Dropped {sum(len(b.records) for b in dropped)} records on {len(dropped)} topics"
        )

    def flush(key: tuple[str, int]) -> None:
        """
        Store buffered records and commit offsets.
        """
        buffer = buffers.pop(key)
        ensure_schema(get_engine(), buffer.schema_id, buffer.schema_str)
        insert_alert_chunk(
            get_engine(),
            get_s3_bucket(),
            buffer.schema_id,
            f"{topic}/{buffer.start_offset.partition:03d}/{buffer.start_offset.offset:020d}-{buffer.end_offset.offset:020d}",
            buffer.records,
            on_complete=partial(
                consumer.commit,
                offsets=[
                    TopicPartition(
                        buffer.end_offset.topic,
                        buffer.end_offset.partition,
                        buffer.end_offset.offset + 1,
                    )
                ],
            )
            if store_offsets
            else None,
        )
        log.info(
            f"Flushed {len(buffer.records)} records; offset now {buffer.end_offset}"
        )

    consumer.subscribe([topic], on_revoke=on_revoke)

    stop = Event()

    def handler(signum, frame):
        log.info(f"Received signal {signum}, exiting")
        log.info(
            f"{sum(len(b.records) for b in buffers.values())} records on {len(buffers)} topics left unflushed"
        )
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, handler)

    interval = 5
    max_tries = max(1, int(timeout / interval / 2))

    while not stop.is_set():
        msg = None
        for _ in range(max_tries):
            msg = consumer.poll(interval)
            if stop.is_set():
                break
        now = datetime.now(UTC)
        if msg is None:
            ...
        elif err := msg.error():
            raise KafkaException(err)
        else:
            offset = TopicPartition(msg.topic(), msg.partition(), msg.offset())
            schema_id, schema, record = unpack(msg)

            key = (msg.topic(), msg.partition())
            # if buffer full or schema changed, upload chunk and reset
            if key in buffers and (
                len(buffers[key].records) >= chunk_size
                or buffers[key].schema_id != schema_id
            ):
                flush(key)
            if key in buffers:
                buffers[key].records.append(record)
                buffers[key].end_offset = offset
                buffers[key].last_seen = now
            else:
                buffers[key] = PartitionBuffer(
                    [record], schema_id, schema, offset, offset, now
                )

        # flush buffers that have been inactive for too long
        for key in list(buffers):
            if (now - buffers[key].last_seen).total_seconds() > timeout:
                log.info(f"Flushing partition {key} due to inactivity")
                flush(key)

    consumer.close()


def run():
    logging.basicConfig(level=logging.INFO)
    typer.run(main)


if __name__ == "__main__":
    run()
