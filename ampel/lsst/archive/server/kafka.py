import json
from functools import cache
from typing import Annotated

from confluent_kafka import serializing_producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    Header,
    HTTPException,
    status,
)
from starlette.concurrency import run_in_threadpool

from .iceberg import (
    AlertRelation,
    StreamQuery,
    table_name_token,
)
from .settings import settings
from .valkey import STREAM_TTL, Valkey

router = APIRouter(tags=["kafka"])


@cache
def get_schema_registry_client():
    return SchemaRegistryClient({"url": str(settings.schema_repository_url)})


def get_serializing_producer(
    x_kafka_username: Annotated[str, Header()],
    x_kafka_password: Annotated[str, Header()],
):
    schema_registry_client = get_schema_registry_client()
    return serializing_producer.SerializingProducer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "security.protocol": "sasl_plaintext",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": x_kafka_username,
            "sasl.password": x_kafka_password,
            "value.serializer": AvroSerializer(
                schema_registry_client,
                conf={
                    "auto.register.schemas": False,
                    "use.latest.version": True,
                    "subject.name.strategy": lambda *args: "alert-packet",
                },
            ),
        }
    )


KafkaProducer = Annotated[
    serializing_producer.SerializingProducer, Depends(get_serializing_producer)
]


def query_to_topic(
    alert_relation: AlertRelation,
    query: StreamQuery,
    topic: str,
    producer: KafkaProducer,
) -> int:
    """
    Create a stream of alerts from the given query and produce them to the given Kafka topic.
    """
    count = 0

    def delivery_callback(err, msg):  # noqa: ARG001
        nonlocal count
        if err is None:
            count += 1

    for chunk in query.execute(alert_relation).to_arrow_reader(
        batch_size=query.chunk_size
    ):
        for record in chunk.to_pylist():
            producer.produce(
                topic=topic,
                value=record,
                on_delivery=delivery_callback,
            )
        producer.flush()
    return count


@router.post(
    "/topic/{topic}/produce",
    tags=["search", "topic"],
    status_code=status.HTTP_202_ACCEPTED,
)
async def stream_query_to_topic(
    topic: str,
    query: Annotated[StreamQuery, Body()],
    alert_relation: AlertRelation,
    tasks: BackgroundTasks,
    valkey: Valkey,
    producer: KafkaProducer,
):
    token = table_name_token()
    key = f"task:{token}"

    async def run_query():
        await valkey.set(key, json.dumps({"status": "running"}), expiry=STREAM_TTL)
        try:
            count = await run_in_threadpool(
                query_to_topic, alert_relation, query, topic, producer
            )
            await valkey.set(
                key,
                json.dumps({"status": "done", "messages": count}),
                expiry=STREAM_TTL,
            )
        except Exception as e:
            await valkey.set(
                key,
                json.dumps({"status": "error", "message": str(e)}),
                expiry=STREAM_TTL,
            )

    tasks.add_task(run_query)

    return {"token": token}


@router.get("/task/{token}/status", tags=["search", "task"])
async def get_task_status(token: str, valkey: Valkey):
    key = f"task:{token}"
    status = await valkey.get(key)
    if status is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"token": token, **json.loads(status)}
