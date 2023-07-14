from typing import Union, Iterable, Optional, List, Literal, Mapping, Tuple, Any, Dict
from deltalake import write_deltalake, DeltaTable
from pathlib import Path
import pyarrow as pa
import pyarrow.fs as pa_fs
from deltalake._internal import DeltaError
from redis.exceptions import RedisError, LockError
from pyarrow.lib import RecordBatchReader
import pyarrow.dataset as ds
from redis import StrictRedis

from redis.lock import Lock
import logging


class RedisLockingObjectStore:
    def __init__(self, redis_client: StrictRedis) -> None:
        self.redis_client = redis_client

    @staticmethod
    def release_lock(acquired_lock: Lock) -> None:
        logging.info("Releasing lock...")
        acquired_lock.release()

    def acquire_lock(self, lock_table_name: str, blocking: bool = True) -> Optional[Lock]:
        lock_obj = Lock(redis=self.redis_client, name=lock_table_name)
        lock_acquired = lock_obj.acquire(blocking=blocking)
        logging.info(f"Trying to acquire lock, blocking: {blocking}")

        if lock_acquired:
            return lock_obj
        else:
            return None


def _get_strict_redis(
    host: str,
    port: int,
    db: int,
):
    return StrictRedis(
        host=host,
        port=port,
        db=db,
    )


def get_store(
    host: str,
    port: int,
    db: int,
) -> RedisLockingObjectStore:
    try:
        redis_client = _get_strict_redis(host=host, port=port, db=db)
        return RedisLockingObjectStore(
            redis_client=redis_client
        )
    except RedisError as redis_error:
        raise redis_error


def write_redis_lock_deltalake(
    self,
    lock_table_name: str,
    table_or_uri: Union[str, Path, DeltaTable],
    data: Union[
        "pd.DataFrame",
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
        acquired_lock = self.acquire_lock(lock_table_name=lock_table_name)

        if acquired_lock:
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
                self.release_lock(acquired_lock)
        else:
            logging.error("Failed to acquire lock. Another process may be holding the lock.")

    except (Exception, ValueError, DeltaError, LockError) as error:
        logging.error(error)
        raise error
