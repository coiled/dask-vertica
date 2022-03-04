from typing import Optional
from hashlib import md5
from datetime import datetime

import pandas as pd


import dask
import dask.dataframe as dd
from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.delayed import delayed
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from dask.utils import SerializableLock

from verticapy import vDataFrame
from verticapy.utilities import pandas_to_vertica, drop, readSQL
import vertica_python
from vertica_python.vertica.connection import Connection
from vertica_python.errors import DuplicateObject


@delayed
def daskdf_to_vertica(
    df: dd.DataFrame,
    connection: Connection,
    name: str,
    schema: str = "public",
    parse_n_lines: Optional[int] = None,
):

    if not parse_n_lines:
        parse_n_lines = df.shape[0]

    if _check_if_exists(connection, name, schema=schema):
        relation_type = "insert"
    else:
        relation_type = "table"

    with SerializableLock(token="daskdf_to_vertica"):
        with connection.cursor() as cursor:
            now = str(datetime.now()).encode()
            now_hash = md5(now).hexdigest()
            tmp = f"tmp_{now_hash}"
            print(tmp)
            vdf = pandas_to_vertica(
                df,
                cursor=cursor,
                name=tmp,
                schema=schema,
                parse_n_lines=parse_n_lines
            )


            vdf.to_db(
                f'"{schema}"."{name}"',
                df.columns.tolist(),
                relation_type=relation_type,
                inplace=False,
            )
            _drop_table(connection, tmp, schema=schema)


@delayed
def ensure_db_exists(
    df: pd.DataFrame,
    connection: Connection,
    name: str,
    schema: str = "public",
    parse_n_lines: int = 10_000,
    inplace: bool = True,
):
    # NOTE: we have a separate `ensure_db_exists` function in order to use
    # pandas' `to_sql` which will create a table if the requested one doesn't
    # already exist.
    with connection.cursor() as cursor:
            vdf = pandas_to_vertica(
                df,
                cursor=cursor,
                name=name,
                schema=schema,
                parse_n_lines=parse_n_lines
            )
            vdf.to_db(
                f'"{schema}"."{name}"',
                df.columns.tolist(),
                relation_type="table",
                inplace=inplace,
            )
            return True


def _check_if_exists(
    connection: Connection,
    name: str,
    schema: str = "public"
) -> bool:
    with connection.cursor() as cur:
        table_query = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME FROM V_CATALOG.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
        """

        all_vtables = readSQL(table_query, cursor=cur)
        does_exist = name in all_vtables["TABLE_NAME"]

    return does_exist


def _drop_table(
    connection: Connection,
    name: str,
    schema: str = "public"
) -> None:
    with connection.cursor() as cur:
        drop(name=f'"{schema}"."{name}"', cursor=cur)


def to_vertica(
    df: dd.DataFrame,
    connection: Connection,
    name: str,
    schema: str = "public",
    if_exists: str = "error",
):
    """Write a Dask DataFrame to a Snowflake table.

    Parameters
    ----------
    df:
        Dask DataFrame to save.
    name:
        Name of the table to save to.
    connection_kwargs:
        Connection arguments used when connecting to Snowflake with
        ``snowflake.connector.connect``.

    Examples
    --------

    >>> from dask_snowflake import to_snowflake
    >>> df = ...  # Create a Dask DataFrame
    >>> to_snowflake(
    ...     df,
    ...     name="my_table",
    ...     connection_kwargs={
    ...         "user": "...",
    ...         "password": "...",
    ...         "account": "...",
    ...     },
    ... )

    """
    # Write the DataFrame meta to ensure table exists before
    # trying to write all partitions in parallel. Otherwise
    # we run into race conditions around creating a new table.
    # Also, some clusters will overwrite the `snowflake.partner` configuration value.
    # We run `ensure_db_exists` on the cluster to ensure we capture the
    # right partner application ID.

    if_exists = if_exists.lower()

    table_already_exists = _check_if_exists(connection, name, schema=schema)

    if not table_already_exists:
        # ensure_db_exists(df.head(), connection, name, schema=schema).compute()
        pass

    else:
        if if_exists == "error":
            raise RuntimeError(f"Table {name} already exists in {schema}")
        elif if_exists == "overwrite":
            _drop_table(connection, name, schema=schema)
            # ensure_db_exists(df.head(), connection, name, schema=schema).compute()

    dask.compute(
        [
            daskdf_to_vertica(partition, connection, name, schema=schema)
            for partition in df.to_delayed()
        ]
    )
