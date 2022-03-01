from functools import partial
from typing import Dict, Optional

import pandas as pd

import dask
import dask.dataframe as dd
from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.delayed import delayed
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from dask.utils import SerializableLock

from verticapy.utilities import pandas_to_vertica
import vertica_python

@delayed
def daskdf_to_vertica(
    df: dd.DataFrame,
    connection_kwargs: Dict,
    name: str,
    schema: str = "public",
    parse_n_lines: int = 10_000,
):

    with vertica_python.connect(**connection_kwargs) as conn:
        with SerializableLock(token="daskdf_to_vertica"):
            with conn.cursor() as cursor:
                vdf = pandas_to_vertica(
                    df,
                    cursor=cursor,
                    name=name,
                    schema=schema,
                    parse_n_lines=parse_n_lines
                )
                vdf.to_db(
                    name,
                    df.columns.tolist(),
                    relation_type="insert",
                    inplace=False,
                )


@delayed
def ensure_db_exists(
    df: pd.DataFrame,
    connection_kwargs: Dict,
    name: str,
    schema: str = "public",
    parse_n_lines: int = 10_000,
):
    # NOTE: we have a separate `ensure_db_exists` function in order to use
    # pandas' `to_sql` which will create a table if the requested one doesn't
    # already exist. H
    with vertica_python.connect(**connection_kwargs) as conn:
        with conn.cursor() as cursor:
            vdf = pandas_to_vertica(
                df,
                cursor=cursor,
                name=name,
                schema=schema,
                parse_n_lines=parse_n_lines
            )
            vdf.to_db(
                name,
                df.columns.tolist(),
                relation_type="table",
                inplace=False,
            )


def to_vertica(
    df: dd.DataFrame,
    connection_kwargs: Dict,
    name: str,
    schema: str = "public",
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
    ensure_db_exists(df._meta, connection_kwargs, name, schema=schema).compute()
    dask.compute(
        [
            daskdf_to_vertica(partition, name, connection_kwargs)
            for partition in df.to_delayed()
        ]
    )
