from redis_lock_object_store import RedisLockingObjectStore, get_store

REDIS_LOCK: RedisLockingObjectStore = get_store()
