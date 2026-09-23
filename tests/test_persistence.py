import datetime
import os

import pytest
import pytest_asyncio
from confluent_kafka import deserializing_consumer, serializing_producer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from fastapi import status
from kafka_mocha.core.kconsumer import KConsumer
from kafka_mocha.core.kproducer import KProducer

from ampel.lsst.archive.server.iceberg import AlertQuery, StreamQuery, table_name_token
from ampel.lsst.archive.server.kafka import get_schema_registry_client
from ampel.lsst.archive.server.models import StreamDescription, StreamRecord
from ampel.lsst.archive.server.valkey import get_valkey_client


@pytest.fixture
def _mock_kafka(monkeypatch):
    """Redefine SerializingProducer and DeserializingConsumer to use KProducer and KConsumer for testing."""
    monkeypatch.setattr(
        "confluent_kafka.serializing_producer.SerializingProducer",
        type(
            "MockSerializingProducer",
            (KProducer,),
            {
                "__init__": serializing_producer.SerializingProducer.__init__,
                "produce": serializing_producer.SerializingProducer.produce,
            },
        ),
    )
    monkeypatch.setattr(
        "confluent_kafka.deserializing_consumer.DeserializingConsumer",
        type(
            "MockDeserializingConsumer",
            (KConsumer,),
            {
                "__init__": deserializing_consumer.DeserializingConsumer.__init__,
                "poll": deserializing_consumer.DeserializingConsumer.poll,
            },
        ),
    )
    # KMessage insists that the value be str|bytes, but cimpl.Message has no such restriction
    monkeypatch.setattr(
        "kafka_mocha.models.kmodels.KMessage.set_value",
        lambda self, value: setattr(self, "_value", value),
    )


def test_persistence(alert_relation, cursor, ensure_table_dirs):
    query = AlertQuery(
        include=["diaSourceId"],
        condition=None,
        limit=1,
    )

    table_name = "persistent_output"
    # XXX HACK: duckdb fileio doesn't create directory structure
    ensure_table_dirs("lsst", table_name)
    query.persist_to(alert_relation, table_name)

    persistent_relation = cursor.sql(f"from {table_name}")
    rows = AlertQuery(
        include=["diaSourceId"],
        condition=None,
        limit=1,
    ).flatten(persistent_relation)
    assert len(rows) == 1


@pytest.mark.usefixtures("_mock_kafka")
@pytest.mark.asyncio(loop_scope="module")
async def test_stream_to_kafka(integration_client):

    query = AlertQuery(
        include=[
            "diaSourceId",
            "target_name",
            "observation_reason",
            "diaSource",
            "diaObject",
            "prvDiaSources",
            "prvDiaForcedSources",
        ],
        condition=None,
        limit=1,
    )

    response = await integration_client.post(
        "/kafka/topic/test/produce",
        json=query.model_dump(mode="json"),
        headers={
            "x-kafka-username": "user",
            "x-kafka-password": "pass",
        },
    )
    response.raise_for_status()
    assert response.status_code == status.HTTP_202_ACCEPTED
    token = response.json()["token"]

    response = await integration_client.get(f"/kafka/task/{token}/status")
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == "done"
    assert response.json()["messages"] == 1
    consumer = deserializing_consumer.DeserializingConsumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-group",
            "value.deserializer": AvroDeserializer(
                get_schema_registry_client(),
                conf={
                    "use.latest.version": True,
                    "subject.name.strategy": lambda *args: "alert-packet",
                },
            ),
        }
    )
    consumer.subscribe(["test"])
    msg = consumer.poll(timeout=1.0)
    assert msg is not None
    assert msg.error() is None
    alert = msg.value()
    assert isinstance(alert, dict)
    assert alert["diaSourceId"] is not None


@pytest_asyncio.fixture(loop_scope="module")
async def stream_token(integration_client, mocker, ensure_table_dirs):
    name = table_name_token()
    mocker.patch(
        "ampel.lsst.archive.server.streams.table_name_token", return_value=name
    )

    query = StreamQuery(
        include=["diaSourceId"],
        condition=None,
        limit=1,
    )
    ensure_table_dirs("lsst", f"stream_{name}")

    response = await integration_client.post(
        "/streams/from_query",
        json=query.model_dump(mode="json"),
    )
    assert response.status_code == status.HTTP_202_ACCEPTED
    assert response.json().keys() == {"resume_token"}
    return response.json()["resume_token"]


@pytest.mark.asyncio(loop_scope="module")
async def test_create_stream(integration_client, stream_token):
    response = await integration_client.get(f"/stream/{stream_token}")
    assert response.status_code == status.HTTP_200_OK
    desc = StreamDescription.model_validate(response.json())
    assert desc.items == 1
    assert desc.remaining == 1
    assert desc.pending == 0


@pytest.mark.asyncio(loop_scope="module")
async def test_delete_stream(integration_client, stream_token, cursor, warehouse_dir):
    entry = (f"stream_{stream_token}",)

    assert entry in cursor.sql("show tables").fetchall()

    response = await integration_client.delete(f"/stream/{stream_token}")
    assert response.status_code == status.HTTP_204_NO_CONTENT

    response = await integration_client.get(f"/stream/{stream_token}")
    assert response.status_code == status.HTTP_404_NOT_FOUND

    assert entry not in cursor.sql("show tables").fetchall()

    table_dir = warehouse_dir / "lsst" / entry[0]
    assert not os.listdir(table_dir / "data")
    assert not os.listdir(table_dir / "metadata")


@pytest.mark.asyncio(loop_scope="module")
async def test_purge_streams(integration_client, stream_token, cursor, warehouse_dir):
    entry = (f"stream_{stream_token}",)

    assert entry in cursor.sql("show tables").fetchall()

    response = await integration_client.post("/streams/purge", follow_redirects=False)
    assert response.status_code == status.HTTP_204_NO_CONTENT

    response = await integration_client.get(f"/stream/{stream_token}")
    assert response.status_code == status.HTTP_200_OK, "stream was not yet expired"

    key = f"stream:{stream_token}"
    valkey = await get_valkey_client()
    model = StreamRecord.model_validate_json(await valkey.get(key))
    model.expires_at = model.started_at - datetime.timedelta(
        days=1
    )  # expire the stream
    await valkey.set(key, model.model_dump_json())

    response = await integration_client.post("/streams/purge", follow_redirects=False)
    assert response.status_code == status.HTTP_204_NO_CONTENT

    response = await integration_client.get(f"/stream/{stream_token}")
    assert response.status_code == status.HTTP_404_NOT_FOUND, "stream was purged"

    assert entry not in cursor.sql("show tables").fetchall()

    table_dir = warehouse_dir / "lsst" / entry[0]
    assert not os.listdir(table_dir / "data")
    assert not os.listdir(table_dir / "metadata")


@pytest.mark.asyncio(loop_scope="module")
async def test_get_chunk(integration_client, stream_token):
    async def description():
        response = await integration_client.get(f"/stream/{stream_token}")
        assert response.status_code == status.HTTP_200_OK
        return StreamDescription.model_validate(response.json())

    # claim a chunk
    response = await integration_client.post(
        f"/stream/{stream_token}/fetch", follow_redirects=False
    )
    assert response.status_code == status.HTTP_303_SEE_OTHER
    location = response.headers["location"]
    assert location == f"/stream/{stream_token}/chunk/0"

    # chunk is claimed
    desc = await description()
    assert desc.pending == 1

    response = await integration_client.post(
        f"/stream/{stream_token}/fetch", follow_redirects=False
    )
    assert response.status_code == status.HTTP_204_NO_CONTENT, (
        "no more unclaimed chunks"
    )

    # get the chunk
    response = await integration_client.get(location)
    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert isinstance(payload, list)
    assert len(payload) == 1

    # release the chunk
    assert (
        await integration_client.post(f"/stream/{stream_token}/chunk/0/release")
    ).status_code == status.HTTP_204_NO_CONTENT

    desc = await description()
    assert desc.pending == 0, "chunk is no longer pending"

    await integration_client.post(
        f"/stream/{stream_token}/fetch", follow_redirects=False
    )

    assert (
        await integration_client.delete(f"/stream/{stream_token}/chunk/0")
    ).status_code == status.HTTP_204_NO_CONTENT

    desc = await description()
    assert desc.remaining == 0, "no chunks left"
    assert desc.pending == 0, "no chunks pending"
