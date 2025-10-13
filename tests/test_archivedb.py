import io

import fastavro
import pytest

from ampel.lsst.archive.avro import extract_record, pack_records
from ampel.lsst.archive.db import insert_alert_chunk, ensure_schema
from ampel.lsst.archive.server.s3 import get_s3_bucket, get_url_for_key
from sqlmodel import create_engine, SQLModel, Session, text

def test_walk_tarball(alert_generator):
    alerts = list(alert_generator())
    assert len(alerts) == 10
    assert alerts[0]["diaSourceId"] == 169359995950333959


def test_pack_alerts(alert_generator):

    alerts, schemas = zip(
        *alert_generator(with_schema=True)
    )

    schema = fastavro.parse_schema(schemas[0])

    packed, ranges = pack_records(schema, alerts)

    reader = fastavro.reader(io.BytesIO(packed), reader_schema=schema)
    all_alerts = list(reader)

    assert len(all_alerts) == len(alerts)
    for orig, alert in zip(alerts, all_alerts):
        assert orig.keys() == alert.keys()
        for k,v in alert.items():
            # NB: approximate comparison with tolerance 0 to allow NaNs to compare equal
            assert orig[k] == pytest.approx(v, abs=0, nan_ok=True)

    for alert, (start, end) in zip(all_alerts, ranges):
        orig = extract_record(io.BytesIO(packed[start:end]), schema=schema)
        assert orig.keys() == alert.keys()
        for k,v in alert.items():
            assert orig[k] == pytest.approx(v, abs=0, nan_ok=True)

def test_insert_alert_chunk(empty_archive, alert_generator, mock_s3_bucket):

    alerts, schemas = zip(
        *alert_generator(with_schema=True)
    )

    engine = create_engine(empty_archive)
    bucket = get_s3_bucket()
    partition, start_offset, end_offset = 0, 4313, 4323
    key = f"lsst-alerts-v9.0/{partition:03d}/{start_offset:020d}-{end_offset:020d}"
    with Session(engine) as session:
        ensure_schema(session, 900, schemas[0])
        insert_alert_chunk(session, bucket, 900, get_url_for_key(bucket, key), alerts)
        session.commit()
    
    with Session(engine) as session:
        result, = session.exec(text("SELECT COUNT(*) FROM alert")).one()
        assert result == 10

        result, = session.exec(text("SELECT COUNT(*) FROM avroschema")).one()
        assert result == 1

        result, = session.exec(text("SELECT COUNT(*) FROM avroblob")).one()
        assert result == 1

