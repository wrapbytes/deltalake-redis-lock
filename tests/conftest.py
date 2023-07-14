import string
from random import choices

from redis import StrictRedis

from src.operators.delta_lock import RedisLockingObjectStore
from src.operators.delta_rs import DeltaRS


def generate_random_string(length):
    return ''.join(choices(string.ascii_lowercase, k=length))


def write_data_no_store(df, table_name):
    delta_rs = DeltaRS(
        endpoint_url="http://localhost:30000"
    )

    delta_rs.write(
        mode="append",
        tier="integration",
        table_name=table_name,
        data=df,
        overwrite_schema=True,
    )


def write_data_to_store(df, table_name):
    redis_client = StrictRedis(
        host="localhost",
        port=32767,
        db=0,
    )

    delta_rs = DeltaRS(
        endpoint_url="http://localhost:30000"
    )

    store = RedisLockingObjectStore(
        delta_rs=delta_rs,
        redis_client=redis_client
    )

    store.write(
        mode="append",
        tier="integration",
        table_name=table_name,
        data=df,
        overwrite_schema=True,
    )
