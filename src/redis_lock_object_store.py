import logging
import os
from datetime import datetime
from typing import Optional

from redis import RedisError, StrictRedis
from redis.lock import Lock


class RedisLockingObjectStore:
    def __init__(self, redis_client: StrictRedis) -> None:
        self.redis_client = redis_client

    @staticmethod
    def release_delta_lock(acquired_lock: Lock) -> None:
        """Release delta table lock.

        Args:
            acquired_lock: Provided Redis Lock

        Returns: None

        """
        logging.info(f"Releasing lock... {datetime.utcnow().isoformat()}")
        acquired_lock.release()

    def acquire_delta_lock(
        self,
        lock_table_name: str,
        blocking: bool = True,
    ) -> Optional[Lock]:
        """Acquire delta table lock.

        Args:
            blocking: Specifies the maximum number of seconds to wait trying to acquire the lock.
            lock_table_name: Name of delta table to lock

        Returns: Optional Redis Lock

        """
        lock_obj = Lock(redis=self.redis_client, name=lock_table_name)
        lock_acquired = lock_obj.acquire(blocking=blocking)

        if lock_acquired:
            logging.info(
                f"Acquired lock lock_table_name: {lock_table_name}, blocking: {blocking}"
            )
            return lock_obj
        else:
            logging.info(f"Did Not Acquire Lock lock_table_name: {lock_table_name}")
            return None


def _get_strict_redis(
    host: str,
    port: int,
    db: int,
) -> StrictRedis:
    """Get Strict Redis Object.

    Args:
        host: Redis host
        port: Redis port
        db: Redis db

    Returns: StrictRedis

    """
    return StrictRedis(
        host=host,
        port=port,
        db=db,
    )


def get_store() -> RedisLockingObjectStore:
    """Get redis lock object store.

    Returns: RedisLockingObjectStore

    """
    try:
        host = os.environ["REDIS_HOST"]
        port = int(os.getenv("REDIS_PORT", 6379))
        db = int(os.getenv("REDIS_DB", 0))

        redis_client = _get_strict_redis(
            host=host,
            port=port,
            db=db,
        )
        return RedisLockingObjectStore(redis_client=redis_client)

    except (KeyError, RedisError) as redis_error:
        raise redis_error
