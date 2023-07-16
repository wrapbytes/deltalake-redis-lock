import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
from deltalake import DeltaTable, write_deltalake
from deltalake._internal import DeltaError
from pyarrow.lib import RecordBatchReader
from redis.exceptions import LockError
from redis.lock import Lock

from global_lock import REDIS_LOCK


def write_redis_lock_deltalake(
    lock_table_name: str,
    table_or_uri: Union[str, Path, DeltaTable],
    data: Union[
        pd.DataFrame,
        pa.Table,
        pa.RecordBatch,
        Iterable[pa.RecordBatch],
        RecordBatchReader,
    ],
    schema: Optional[pa.Schema] = None,
    partition_by: Optional[Union[List[str], str]] = None,
    filesystem: Optional[pa_fs.FileSystem] = None,
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    file_options: Optional[ds.ParquetFileWriteOptions] = None,
    max_partitions: Optional[int] = None,
    max_open_files: int = 1024,
    max_rows_per_file: int = 10 * 1024 * 1024,
    min_rows_per_group: int = 64 * 1024,
    max_rows_per_group: int = 128 * 1024,
    name: Optional[str] = None,
    description: Optional[str] = None,
    configuration: Optional[Mapping[str, Optional[str]]] = None,
    overwrite_schema: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
    partition_filters: Optional[List[Tuple[str, str, Any]]] = None,
) -> None:
    try:
        acquired_lock: Optional[Lock] = REDIS_LOCK.acquire_delta_lock(
            lock_table_name=lock_table_name
        )
        logging.info(f"Acquired Redis Lock...")

        if isinstance(acquired_lock, Lock):
            try:
                logging.info("Lock acquired. Writing to Delta...")
                return write_deltalake(
                    table_or_uri=table_or_uri,
                    data=data,
                    schema=schema,
                    partition_by=partition_by,
                    filesystem=filesystem,
                    mode=mode,
                    file_options=file_options,
                    max_partitions=max_partitions,
                    max_open_files=max_open_files,
                    max_rows_per_file=max_rows_per_file,
                    min_rows_per_group=min_rows_per_group,
                    max_rows_per_group=max_rows_per_group,
                    name=name,
                    description=description,
                    configuration=configuration,
                    overwrite_schema=overwrite_schema,
                    storage_options=storage_options,
                    partition_filters=partition_filters,
                )
            finally:
                REDIS_LOCK.release_delta_lock(acquired_lock=acquired_lock)
        else:
            logging.error("Failed to acquire lock. Another process may be holding the lock.")

    except (Exception, ValueError, DeltaError, LockError) as error:
        logging.error(error)
        raise error
