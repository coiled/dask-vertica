import os

import pandas as pd
import pytest
from vertica_python import connect

import dask.dataframe as dd
from distributed import Client

from dask_vertica import read_vertica, to_vertica
from dask_vertica.core import _drop_table


@pytest.fixture
def client():
    with Client(n_workers=2, threads_per_worker=10) as client:
        yield client


@pytest.fixture
def schema():
    return os.environ["VERTICA_SCHEMA"]


@pytest.fixture(scope="module")
def connection_kwargs():
    return dict(
        host=os.environ["VERTICA_HOST"],
        port=os.environ["VERTICA_PORT"],
        user=os.environ["VERTICA_USER"],
        password=os.environ["VERTICA_PASSWORD"],
        database=os.environ["VERTICA_DB"],
        connection_load_balance=True,
        session_label="py",
        unicode_error="strict",
    )


@pytest.fixture
def small_df():
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlene", "Dale", "Emily"],
            "id": [1, 2, 3, 4, 5],
            "height": [178.5, 180, 160, 185.5, 170],
        }
    )
    return dd.from_pandas(df, npartitions=1)


@pytest.fixture
def demo_ts():
    return dd.demo.make_timeseries(
        start="2000-01-01", end="2000-12-31", freq="30s", partition_freq="1W"
    ).reset_index()


@pytest.fixture
def remove_test_tables(connection_kwargs, schema):
    with connect(**connection_kwargs) as connection:
        print("setup - dropping testing_small_df")
        _drop_table(connection, "testing_small_df", schema=schema)
        print("setup - dropping testing_if_exists_df")
        _drop_table(connection, "testing_if_exists_df", schema=schema)
        print("setup - dropping testing_if_exists_insert")
        _drop_table(connection, "testing_if_exists_insert", schema=schema)

    yield

    with connect(**connection_kwargs) as connection:
        print("teardown - dropping testing_small_df")
        _drop_table(connection, "testing_small_df", schema=schema)
        print("teardown - dropping testing_if_exists_df")
        _drop_table(connection, "testing_if_exists_df", schema=schema)
        print("teardown - dropping testing_if_exists_insert")
        _drop_table(connection, "testing_if_exists_insert", schema=schema)


def test_write_read_roundtrip(
    remove_test_tables, small_df, connection_kwargs, client, schema
):
    table_name = "testing_small_df"
    to_vertica(small_df, connection_kwargs, name=table_name, schema=schema)

    ddf_out = read_vertica(connection_kwargs, table_name, npartitions=1, schema=schema)

    result = ddf_out.sort_values(by="id")
    dd.utils.assert_eq(small_df, result, check_index=False, check_divisions=False)


def test_write_if_exists_error(
    small_df, connection_kwargs, client, schema, remove_test_tables
):
    table_name = "testing_if_exists_df"
    to_vertica(small_df, connection_kwargs, name=table_name, schema=schema)

    with pytest.raises(RuntimeError):
        to_vertica(
            small_df,
            connection_kwargs,
            name=table_name,
            schema=schema,
            if_exists="error",
        )

    df_out = read_vertica(
        connection_kwargs, table_name, npartitions=2, schema=schema
    ).compute()
    assert df_out.shape[0] == 5


def test_write_if_exists_overwrite(
    small_df, connection_kwargs, client, schema, remove_test_tables
):

    table_name = "testing_if_exists_df"
    to_vertica(
        small_df,
        connection_kwargs,
        name=table_name,
        schema=schema,
        if_exists="overwrite",
    )
    to_vertica(
        small_df,
        connection_kwargs,
        name=table_name,
        schema=schema,
        if_exists="overwrite",
    )

    df_out = read_vertica(
        connection_kwargs, table_name, npartitions=2, schema=schema
    ).compute()
    assert df_out.shape[0] == 5


def test_write_if_exists_insert(
    small_df, connection_kwargs, client, schema, remove_test_tables
):
    table_name = "testing_if_exists_insert"
    to_vertica(
        small_df,
        connection_kwargs,
        name=table_name,
        schema=schema,
        if_exists="overwrite",
    )

    to_vertica(
        small_df,
        connection_kwargs,
        name=table_name,
        schema=schema,
        if_exists="insert",
    )

    df_out = read_vertica(
        connection_kwargs, table_name, npartitions=2, schema=schema
    ).compute()
    assert df_out.shape[0] == 10
