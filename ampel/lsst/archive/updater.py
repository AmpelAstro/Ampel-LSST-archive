import typer
from typing import Annotated

from confluent_kafka import Consumer, TopicPartition, KafkaError, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext
from confluent_kafka.avro import AvroDeserializer


def _raise_errors(self, exc: Exception) -> None:
    raise exc


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
    chunk_size: Annotated[int, typer.Option()] = 1000,
    timeout: Annotated[float, typer.Option()] = 300.0,
):
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": username,
            "sasl.password": password,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": False,
            "error_cb": _raise_errors,
        }
    )
    deserializer = AvroDeserializer(
        SchemaRegistryClient(
            {
                "url": registry,
            }
        )
    )

    from .server.settings import settings

    consumer.subscribe([topic])

    buffer: dict[TopicPartition, list[Message]] = {}

    while True:
        msg = consumer.poll(timeout)
        if msg is None:
            print("No message received within timeout, exiting")
            break
        if msg.error():
            raise msg.error()

        tp = TopicPartition(msg.topic(), msg.partition())
        record = deserializer(msg.value(), SerializationContext(msg.topic(), "value"))
        break


if __name__ == "__main__":
    typer.run(main)
