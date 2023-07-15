import logging
import os
import random
import string
from typing import Generator
from unittest.mock import MagicMock, call, patch

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from redis.lock import Lock

from redis_lock_object_store import RedisLockingObjectStore
from src.deltalake_redis_lock import write_redis_lock_deltalake

REDIS_LOCK_PATH = "src.deltalake_redis_lock.REDIS_LOCK"


@pytest.fixture(scope="function")
def mock_data() -> pa.Table:
    random.seed(42)  # Set a seed for reproducibility

    num_rows = random.randint(0, 1000)
    data = []

    for _ in range(num_rows):
        row = {
            "id": random.randint(0, 100),
            "name": "".join(random.choices(string.ascii_letters, k=5)),
            "age": random.randint(20, 60),
        }
        data.append(row)

    # Define the schema
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("age", pa.int64()),
        ]
    )

    # Convert the data to PyArrow Arrays
    arrays = [
        pa.array([row["id"] for row in data]),
        pa.array([row["name"] for row in data]),
        pa.array([row["age"] for row in data]),
    ]

    # Create the PyArrow Table from the arrays
    yield pa.Table.from_arrays(arrays, schema=schema)


@pytest.fixture(scope="function")
def mock_redis_lock() -> Generator[MagicMock, None, None]:
    with patch(REDIS_LOCK_PATH, spec=RedisLockingObjectStore) as mock_locking_object_store:
        mock_locking_object_store.acquire_delta_lock.return_value = MagicMock(spec=Lock)

        yield mock_locking_object_store


@pytest.mark.parametrize("lock_table_name", ["test_table"])
def test_write_redis_lock_deltalake(mock_data, mock_redis_lock, lock_table_name):
    table_path = f"{os.getcwd()}/{lock_table_name}"
    write_redis_lock_deltalake(
        mode="overwrite",
        lock_table_name=lock_table_name,
        table_or_uri=table_path,
        data=mock_data,
    )

    assert mock_redis_lock.acquire_delta_lock.call_args_list == [
        call(lock_table_name=lock_table_name)
    ]

    assert mock_redis_lock.acquire_delta_lock.call_count == 1

    assert mock_redis_lock.release_delta_lock.call_args_list == [
        call(acquired_lock=mock_redis_lock.acquire_delta_lock.return_value)
    ]

    assert mock_redis_lock.release_delta_lock.call_count == 1

    delta_table = DeltaTable(
        table_uri=table_path,
    )

    table = delta_table.to_pyarrow_table()

    assert len(table.schema.names) == len(mock_data.schema.names)
    assert table.equals(mock_data)

    delta_table.vacuum(
        retention_hours=0,
        enforce_retention_duration=False,
        dry_run=False,
    )
