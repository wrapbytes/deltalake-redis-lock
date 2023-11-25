import logging
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
from deltalake import DeltaTable, write_deltalake
from deltalake._internal import DeltaError
from deltalake.table import FilterType
from pyarrow.lib import RecordBatchReader
from redis.exceptions import LockError
from redis.lock import Lock

from global_lock import REDIS_LOCK


def optimize_redis_lock_deltalake(
    lock_table_name: str,
    table_or_uri: Union[str, Path, DeltaTable],
    storage_options: Optional[Dict[str, str]] = None,
    partition_filters: Optional[FilterType] = None,
    target_size: Optional[int] = None,
    max_concurrent_tasks: Optional[int] = None,
    retention_hours: Optional[int] = None,
    dry_run: bool = True,
    enforce_retention_duration: bool = True,
) -> None:
    """
    Optimize a Delta table with Redis lock.

    Args:
        lock_table_name: The name of the lock table.
        table_or_uri: The Delta table or URI to optimize.
        storage_options: Additional storage options for the Delta table.
        partition_filters: Filters to apply when optimizing the table.
        target_size: The target size for compacting the table.
        max_concurrent_tasks: The maximum number of concurrent optimization tasks.
        retention_hours: The number of hours to retain old versions of the table.
        dry_run: Whether to perform a dry run of the optimization.
        enforce_retention_duration: Whether to enforce the retention duration.

    """

    def optimize_delta():
        _optimize_delta_table(
            table_or_uri=table_or_uri,
            storage_options=storage_options,
            partition_filters=partition_filters,
            target_size=target_size,
            max_concurrent_tasks=max_concurrent_tasks,
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=enforce_retention_duration,
        )

    logging.info(f"Perform Optimize Delta lock_table_name: {lock_table_name}")
    _execute_with_redis_lock(lock_table_name, optimize_delta)


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
    """
    Write data to a Delta table with Redis lock.

    Args:
        lock_table_name: The name of the lock table.
        table_or_uri: The Delta table or URI to write the data to.
        data: The data to write to the Delta table.
        schema: The schema of the data.
        partition_by: The column(s) to partition the Delta table by.
        filesystem: The file system to use for writing the data.
        mode: The write mode for the Delta table.
        file_options: Additional file write options for the Delta table.
        max_partitions: The maximum number of partitions.
        max_open_files: The maximum number of open files.
        max_rows_per_file: The maximum number of rows per file.
        min_rows_per_group: The minimum number of rows per group.
        max_rows_per_group: The maximum number of rows per group.
        name: The name of the Delta table.
        description: The description of the Delta table.
        configuration: Additional configuration for the Delta table.
        overwrite_schema: Whether to overwrite the existing schema.
        storage_options: Additional storage options for the Delta table.
        partition_filters: Filters to apply when writing the data.

    """

    def write_delta():
        write_deltalake(
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

    logging.info(f"Perform Delta Write: {lock_table_name}")
    _execute_with_redis_lock(lock_table_name, write_delta)


def _execute_with_redis_lock(
    lock_table_name: str,
    function: Callable[..., Any],
    *args,
    **kwargs,
) -> Any:
    """
    Executes a function with Redis lock.

    Args:
        lock_table_name: The name of the lock table.
        function: A callable function that takes any number of arguments and returns any type.
        *args: Additional positional arguments to be passed to the function.
        **kwargs: Additional keyword arguments to be passed to the function.

    Returns:
        The result returned by the executed function.

    Raises:
        Exception: If any error occurs during execution.

    """
    try:
        logging.info(f"Try to Acquire Redis Lock...{lock_table_name}")
        acquired_lock: Optional[Lock] = REDIS_LOCK.acquire_delta_lock(
            lock_table_name=lock_table_name
        )

        if isinstance(acquired_lock, Lock):
            logging.info(f"Acquired Redis Lock...{lock_table_name}")

            try:
                logging.info("Executing function...")
                return function(*args, **kwargs)
            finally:
                REDIS_LOCK.release_delta_lock(acquired_lock=acquired_lock)
        else:
            logging.error(
                "Failed to acquire lock. Another process may be holding the lock."
            )

    except (Exception, DeltaError, LockError) as error:
        logging.error(error)
        raise error


def _optimize_delta_table(
    table_or_uri: Union[str, Path, DeltaTable],
    storage_options: Optional[Dict[str, str]] = None,
    partition_filters: Optional[FilterType] = None,
    target_size: Optional[int] = None,
    max_concurrent_tasks: Optional[int] = None,
    retention_hours: Optional[int] = None,
    dry_run: bool = True,
    enforce_retention_duration: bool = True,
) -> None:
    """
    Optimize a Delta table with Redis lock.

    Args:
        table_or_uri: The Delta table or URI to optimize.
        storage_options: Additional storage options for the Delta table.
        partition_filters: Filters to apply when optimizing the table.
        target_size: The target size for compacting the table.
        max_concurrent_tasks: The maximum number of concurrent optimization tasks.
        retention_hours: The number of hours to retain old versions of the table.
        dry_run: Whether to perform a dry run of the optimization.
        enforce_retention_duration: Whether to enforce the retention duration.

    """
    delta_table = DeltaTable(table_uri=table_or_uri, storage_options=storage_options)
    delta_table.optimize.compact(
        partition_filters=partition_filters,
        target_size=target_size,
        max_concurrent_tasks=max_concurrent_tasks,
    )
    delta_table.vacuum(
        retention_hours=retention_hours,
        dry_run=dry_run,
        enforce_retention_duration=enforce_retention_duration,
    )
