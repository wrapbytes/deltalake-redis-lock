from typing import Union, Iterable, Optional
from redis import StrictRedis
from redis.lock import Lock
from pandas import DataFrame
from pyarrow import Table, RecordBatch
import logging
from src.delta_rs import write_redis_lock_deltalake


class RedisLockingObjectStore:
    def __init__(self, redis_client: StrictRedis) -> None:
        self.redis_client = redis_client

    @staticmethod
    def release_lock(acquired_lock: Lock) -> None:
        logging.info("Releasing lock...")
        acquired_lock.release()

    def acquire_lock(self, table_name: str, blocking: bool = True) -> Optional[Lock]:
        lock_obj = Lock(self.redis_client, table_name)
        lock_acquired = lock_obj.acquire(blocking=blocking)
        logging.info(f"Trying to acquire lock, blocking: {blocking}")

        if lock_acquired:
            return lock_obj
        else:
            return None

    def write(
        self,
        mode: str,
        tier: str,
        table_name: str,
        data: Union[DataFrame, Table, RecordBatch, Iterable[RecordBatch]],
        overwrite_schema: bool = False,
    ):
        acquired_lock = self.acquire_lock(table_name=table_name)

        if acquired_lock:
            try:
                logging.info("Lock acquired. Writing to Delta...")
                return write_redis_lock_deltalake(
                    mode=mode,
                    data=data,
                    overwrite_schema=overwrite_schema,
                )
            finally:
                self.release_lock(acquired_lock)
        else:
            raise ValueError("Failed to acquire lock. Another process may be holding the lock.")
