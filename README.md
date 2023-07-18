# deltalake-redis-lock

![example workflow](https://github.com/wrapbytes/deltalake-redis-lock/actions/workflows/merge.yaml/badge.svg)
![example workflow](https://github.com/wrapbytes/deltalake-redis-lock/actions/workflows/pr.yaml/badge.svg)

A library creating an interface for a write lock for [delta-rs](https://pypi.org/project/deltalake/).

## Library Usage

When using this client, it can be used from multiple hosts. Below follow a minimal example
to mimic this behaviour.

### Redis Env Variables

Make sure to set these `envs` before executing code.
```bash
REDIS_HOST=<host>
REDIS_PORT=<port>  # default 6739
REDIS_DB=<0>  # default 0
```

### Concurrent Write Example

```python
# run.py
import logging
import os
from multiprocessing import Pool

from deltalake import DeltaTable
from pandas import DataFrame

from deltalake_redis_lock import write_redis_lock_deltalake, optimize_redis_lock_deltalake

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def fake_worker(args) -> None:
    df, table_name = args
    table_path = f"{os.getcwd()}/{table_name}"

    write_redis_lock_deltalake(
        table_or_uri=table_path,
        lock_table_name=table_name,
        mode="append",
        data=df,
        overwrite_schema=True,
    )


def define_datasets(_table_name: str) -> None:
    df1 = DataFrame({"id": [1]})
    df2 = DataFrame({"id": [2]})
    df3 = DataFrame({"id": [3]})
    df4 = DataFrame({"id": [4]})

    datasets = [(df1, table_name), (df2, table_name), (df3, table_name), (df4, table_name)]

    with Pool() as pool:
        pool.map(fake_worker, datasets)


if __name__ == '__main__':
    table_name = f"test_run"
    table_path = f"{os.getcwd()}/{table_name}"

    define_datasets(_table_name=table_name)

    df = DeltaTable(table_uri=table_path).to_pandas().to_string()
    logging.info(df)
```

```bash
2023-07-18 21:28:47 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:28:47 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:28:47 [INFO] Acquired lock, blocking: True
2023-07-18 21:28:47 [INFO] Acquired Redis Lock...
2023-07-18 21:28:47 [INFO] Lock acquired. Executing function...
2023-07-18 21:28:47 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:28:47 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:28:47 [INFO] Releasing lock... 2023-07-18T20:28:47.378630
2023-07-18 21:28:47 [INFO] Acquired lock, blocking: True
2023-07-18 21:28:47 [INFO] Acquired Redis Lock...
2023-07-18 21:28:47 [INFO] Lock acquired. Executing function...
2023-07-18 21:28:47 [INFO] Releasing lock... 2023-07-18T20:28:47.419373
2023-07-18 21:28:47 [INFO] Acquired lock, blocking: True
2023-07-18 21:28:47 [INFO] Acquired Redis Lock...
2023-07-18 21:28:47 [INFO] Lock acquired. Executing function...
2023-07-18 21:28:47 [INFO] Releasing lock... 2023-07-18T20:28:47.476411
2023-07-18 21:28:47 [INFO] Acquired lock, blocking: True
2023-07-18 21:28:47 [INFO] Acquired Redis Lock...
2023-07-18 21:28:47 [INFO] Lock acquired. Executing function...
2023-07-18 21:28:47 [INFO] Releasing lock... 2023-07-18T20:28:47.517992
   id
0   1
1   3
2   2
3   4
```

**Structure**

```bash
test_run
├── 0-a2811af1-e9fa-4984-9824-3956acdbaba8-0.parquet
├── 1-87889b2d-1971-4e9b-8244-5e0d4a222458-0.parquet
├── 2-a2f0ac25-df02-43b7-945d-014db522b19f-0.parquet
├── 3-e57eae65-3cc7-4539-9eb6-b41ba52642bc-0.parquet
└── _delta_log
    ├── 00000000000000000000.json
    ├── 00000000000000000001.json
    ├── 00000000000000000002.json
    └── 00000000000000000003.json

1 directory, 8 files
```

### Concurrent Write With Optimize Example
```python
# run.py
import logging
import os
from multiprocessing import Pool

from deltalake import DeltaTable
from pandas import DataFrame

from deltalake_redis_lock import write_redis_lock_deltalake, optimize_redis_lock_deltalake

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def fake_worker(args) -> None:
    df, table_name = args
    table_path = f"{os.getcwd()}/{table_name}"

    write_redis_lock_deltalake(
        table_or_uri=table_path,
        lock_table_name=table_name,
        mode="append",
        data=df,
        overwrite_schema=True,
    )

    optimize_redis_lock_deltalake(
        table_or_uri=table_path,
        lock_table_name=table_name,
        retention_hours=0,
        dry_run=False,
        enforce_retention_duration=False,
    )


def define_datasets(_table_name: str) -> None:
    df1 = DataFrame({"id": [1]})
    df2 = DataFrame({"id": [2]})
    df3 = DataFrame({"id": [3]})
    df4 = DataFrame({"id": [4]})

    datasets = [(df1, table_name), (df2, table_name), (df3, table_name), (df4, table_name)]

    with Pool() as pool:
        pool.map(fake_worker, datasets)


if __name__ == '__main__':
    table_name = f"test_run"
    table_path = f"{os.getcwd()}/{table_name}"

    define_datasets(_table_name=table_name)

    df = DeltaTable(table_uri=table_path).to_pandas().to_string()
    logging.info(df)
```

**Output**
```bash
2023-07-18 21:26:42 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:26:42 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:26:42 [INFO] Acquired lock, blocking: True
2023-07-18 21:26:42 [INFO] Acquired Redis Lock...
2023-07-18 21:26:42 [INFO] Lock acquired. Executing function...
2023-07-18 21:26:42 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:26:42 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:26:42 [INFO] Releasing lock... 2023-07-18T20:26:42.681030
2023-07-18 21:26:42 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:26:42 [INFO] Acquired lock, blocking: True
2023-07-18 21:26:42 [INFO] Acquired Redis Lock...
2023-07-18 21:26:42 [INFO] Lock acquired. Executing function...
2023-07-18 21:26:42 [INFO] Releasing lock... 2023-07-18T20:26:42.689819
2023-07-18 21:26:42 [INFO] Acquired lock, blocking: True
2023-07-18 21:26:42 [INFO] Acquired Redis Lock...
2023-07-18 21:26:42 [INFO] Lock acquired. Executing function...
2023-07-18 21:26:42 [INFO] Releasing lock... 2023-07-18T20:26:42.750781
2023-07-18 21:26:42 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:26:42 [INFO] Acquired lock, blocking: True
2023-07-18 21:26:42 [INFO] Acquired Redis Lock...
2023-07-18 21:26:42 [INFO] Lock acquired. Executing function...
2023-07-18 21:26:42 [INFO] Releasing lock... 2023-07-18T20:26:42.760280
2023-07-18 21:26:42 [INFO] Acquired lock, blocking: True
2023-07-18 21:26:42 [INFO] Acquired Redis Lock...
2023-07-18 21:26:42 [INFO] Lock acquired. Executing function...
2023-07-18 21:26:42 [INFO] Releasing lock... 2023-07-18T20:26:42.866534
2023-07-18 21:26:42 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:26:42 [INFO] Acquired lock, blocking: True
2023-07-18 21:26:42 [INFO] Acquired Redis Lock...
2023-07-18 21:26:42 [INFO] Lock acquired. Executing function...
2023-07-18 21:26:42 [INFO] Releasing lock... 2023-07-18T20:26:42.882519
2023-07-18 21:26:42 [INFO] Acquired lock, blocking: True
2023-07-18 21:26:42 [INFO] Acquired Redis Lock...
2023-07-18 21:26:42 [INFO] Lock acquired. Executing function...
2023-07-18 21:26:42 [INFO] Releasing lock... 2023-07-18T20:26:42.985008
2023-07-18 21:26:42 [INFO] Try to Acquire Redis Lock...
2023-07-18 21:26:42 [INFO] Acquired lock, blocking: True
2023-07-18 21:26:42 [INFO] Acquired Redis Lock...
2023-07-18 21:26:42 [INFO] Lock acquired. Executing function...
2023-07-18 21:26:43 [INFO] Releasing lock... 2023-07-18T20:26:43.000558
   id
0   4
1   3
2   1
3   2
```

**Structure**

```bash
test_run
└── _delta_log
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   ├── 00000000000000000002.json
│   ├── 00000000000000000003.json
│   ├── 00000000000000000004.json
│   ├── 00000000000000000005.json
│   └── 00000000000000000006.json
└──part-00001-a13ca1fe-0a52-44c2-b2ce-b7eb95704536-c000.zstd.parquet

1 directory, 8 files
```

This can be executed with something like:

```bash
seq 2 | xargs -I{} -P 2 poetry run python run.py
```

## Setup From Scratch

### Requirement

* ^python3.9
* poetry 1.1.13
* make (GNU Make 3.81)

### Setup

```bash
make setup-environment
```

Update package
```bash
make update
```

### Test

```bash
export PYTHONPATH="${PYTHONPATH}:src"
make test type=unit
```

### Docker

The reason `docker` is used in the source code here, is to be able to build up an encapsulated
environment of the codebase, and do `unit/integration and load tests`.

```bash
make build-container-image DOCKER_BUILD="buildx build --platform linux/amd64" CONTEXT=.
```

```bash
make run-container-tests type=unit
```
