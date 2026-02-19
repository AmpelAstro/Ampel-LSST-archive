import itertools
import logging
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING

from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.io.pyarrow import (
    ArrowScan,
    _dataframe_to_data_files,
)
from pyiceberg.manifest import DataFile
from pyiceberg.table import ALWAYS_TRUE, Table
from pyiceberg.table.refs import MAIN_BRANCH
from pyiceberg.typedef import EMPTY_DICT

if TYPE_CHECKING:
    import pyarrow as pa

log = logging.getLogger(__name__)


def update_table_files(
    table: "Table",
    transform_function: "Callable[[pa.Table], pa.Table]",
    file_filter: str | BooleanExpression = ALWAYS_TRUE,
    snapshot_properties=EMPTY_DICT,
    branch=MAIN_BRANCH,
    limit: int | None = None,
):
    """
    Hobo full-file update for Iceberg tables, adapted from
    pyiceberg.Transaction.delete

    For each file that could contain rows matching `file_filter`, read the
    entire file, apply `transform_function` to the resulting Arrow Table, and
    write out new data files with the transformed data. Finally, update the
    table snapshot to replace the original data files with the newly written
    ones.
    """
    with table.transaction() as txn:
        file_scan = table.scan(row_filter=file_filter)
        files = list(file_scan.plan_files())

        log.info(f"Found {len(files)} files to update")

        commit_uuid = uuid.uuid4()
        counter = itertools.count(0)

        replaced_files: list[tuple[DataFile, list[DataFile]]] = []

        try:
            for i, original_file in enumerate(files):
                df = ArrowScan(
                    table_metadata=txn.table_metadata,
                    io=table.io,
                    projected_schema=txn.table_metadata.schema(),
                    row_filter=AlwaysTrue(),
                ).to_table(tasks=[original_file])
                log.debug(
                    f"Got {len(df)} records from {original_file.file} ({i + 1}/{len(files)})"
                )
                new_df = transform_function(df)
                replaced_files.append(
                    (
                        original_file.file,
                        list(
                            _dataframe_to_data_files(
                                io=table.io,
                                df=new_df,
                                table_metadata=txn.table_metadata,
                                write_uuid=commit_uuid,
                                counter=counter,
                            )
                        ),
                    )
                )

                if limit is not None and (i + 1) >= limit:
                    log.info(f"Reached limit of {limit} files, stopping.")
                    break
        finally:
            if len(replaced_files) > 0:
                with txn.update_snapshot(
                    snapshot_properties=snapshot_properties, branch=branch
                ).overwrite() as overwrite_snapshot:
                    overwrite_snapshot.commit_uuid = commit_uuid
                    for original_data_file, replaced_data_files in replaced_files:
                        overwrite_snapshot.delete_data_file(original_data_file)
                        for replaced_data_file in replaced_data_files:
                            overwrite_snapshot.append_data_file(replaced_data_file)

        return replaced_files
