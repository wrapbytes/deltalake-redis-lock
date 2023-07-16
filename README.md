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
import string
from random import choices
from multiprocessing import Pool

from pandas import DataFrame

from deltalake_redis_lock import write_redis_lock_deltalake

def fake_worker(args):
    df, table_name = args

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    write_redis_lock_deltalake(
        table_or_uri=f"{os.getcwd()}/{table_name}",
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


def generate_random_string(length):
    return "".join(choices(string.ascii_lowercase, k=length))


if __name__ == '__main__':
    random_string = generate_random_string(3)
    table_name = f"test_run_{random_string}"

    define_datasets(_table_name=table_name)
```

This can be exeucted with something like:

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
