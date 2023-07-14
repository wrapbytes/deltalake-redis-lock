from joblib import Parallel, delayed
from pandas import DataFrame

from tests.conftest import write_data_to_store, generate_random_string


def test_redis_locking_object_joblib_store(_table_name: str) -> None:
    df1 = DataFrame({'id': [1]})
    df2 = DataFrame({'id': [2]})
    df3 = DataFrame({'id': [3]})
    df4 = DataFrame({'id': [4]})

    Parallel(n_jobs=-1)(
        delayed(write_data_to_store)(
            df=df,
            table_name=_table_name,
        )
        for df in [df1, df2, df3, df4]
    )


random_string = generate_random_string(3)
table_name = f"test_run_{random_string}"

Parallel(n_jobs=-1, backend="multiprocessing")(
    delayed(test_redis_locking_object_joblib_store)(table_name=table_name)
    for _ in [1, 2]
)
