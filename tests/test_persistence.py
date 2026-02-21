from ampel.lsst.archive.server.iceberg import (
    AlertQuery,
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
