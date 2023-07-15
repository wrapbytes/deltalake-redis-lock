import os
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from redis.lock import Lock

STRICT_REDIS_PATH = "src.deltalake_redis_lock"


@pytest.fixture(scope="function")
def mock_lock() -> Generator[MagicMock, None, None]:
    with patch(f"{STRICT_REDIS_PATH}.REDIS_LOCK", spec=Lock) as mock_lock:
        yield mock_lock
