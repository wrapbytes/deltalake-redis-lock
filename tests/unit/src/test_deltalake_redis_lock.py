import os
import random
import shutil
import string
from typing import Generator
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from redis.lock import Lock

from src.deltalake_redis_lock import (
    optimize_redis_lock_deltalake,
    write_redis_lock_deltalake,
)
from src.redis_lock_object_store import RedisLockingObjectStore

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
    with patch(
        REDIS_LOCK_PATH, spec=RedisLockingObjectStore
    ) as mock_locking_object_store:
        mock_locking_object_store.acquire_delta_lock.return_value = MagicMock(spec=Lock)

        yield mock_locking_object_store


def count_files_in_directory(directory: str) -> int:
    return sum(
        1
        for entry in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, entry))
    )


@pytest.mark.parametrize("lock_table_name", ["test_table"])
def test_write_redis_lock_deltalake_with_optimize(
    mock_data, mock_redis_lock, lock_table_name
):
    table_path = f"{os.getcwd()}/{lock_table_name}"
    write_redis_lock_deltalake(
        mode="overwrite",
        lock_table_name=lock_table_name,
        table_or_uri=table_path,
        data=mock_data,
    )

    mock_redis_lock.acquire_delta_lock.assert_called_once_with(
        lock_table_name=lock_table_name
    )
    mock_redis_lock.release_delta_lock.assert_called_once_with(
        acquired_lock=mock_redis_lock.acquire_delta_lock.return_value
    )

    write_redis_lock_deltalake(
        mode="append",
        lock_table_name=lock_table_name,
        table_or_uri=table_path,
        data=mock_data,
    )

    assert mock_redis_lock.acquire_delta_lock.call_count == 2
    assert mock_redis_lock.release_delta_lock.call_count == 2

    delta_table = DeltaTable(table_uri=table_path)
    table = delta_table.to_pyarrow_table()
    union_mock_data = pa.concat_tables([mock_data, mock_data])
    assert table.equals(union_mock_data)
    assert len(delta_table.files()) == 2

    optimize_redis_lock_deltalake(
        table_or_uri=table_path,
        lock_table_name=lock_table_name,
        retention_hours=1,
        dry_run=False,
        enforce_retention_duration=False,
    )

    delta_table = DeltaTable(table_uri=table_path)
    # Delta
    # --------
    # 1 overwrite
    # 1 append
    # 1 optimize
    # --------
    # tot: 1 file for delta table
    assert len(delta_table.files()) == 1

    # Physical
    # --------
    # 1 overwrite
    # 1 append
    # 1 optimize
    # --------
    # tot: 3 files physically still in storage
    assert count_files_in_directory(table_path) == 3

    optimize_redis_lock_deltalake(
        table_or_uri=table_path,
        lock_table_name=lock_table_name,
        retention_hours=0,
        dry_run=False,
        enforce_retention_duration=False,
    )
    delta_table = DeltaTable(table_uri=table_path)

    ###
    # 1 overwrite
    # 1 append
    # 1 optimize
    #
    # vacuum (remove overwrite, and append, keep optimized of both)
    #
    # tot: 1 file in storage
    ###
    assert count_files_in_directory(table_path) == 1

    table = delta_table.to_pyarrow_table()
    union_mock_data = pa.concat_tables([mock_data, mock_data])
    assert table.equals(union_mock_data)

    shutil.rmtree(table_path)
