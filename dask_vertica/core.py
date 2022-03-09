import logging
import uuid
from functools import partial
from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
from vertica_python import connect
from vertica_python.vertica.connection import Connection
from verticapy.utilities import drop, pandas_to_vertica, readSQL
from verticapy.vdataframe import vDataFrame

import dask
import dask.dataframe as dd
from dask.base import tokenize
from dask.delayed import delayed
from dask.utils import SerializableLock


@delayed
def daskdf_to_vertica(
    df: dd.DataFrame,
    connection_kwargs: Dict,
    name: str,
    schema: str = "public",
    relation_type: str = "insert",
    parse_n_lines: Optional[int] = None,
) -> None:
    """Upload a dask dataframe to Vertica

    Parameters
    ----------
    df : dask.DataFrame
    connection_kwargs : dict
        Connection arguments used when connecting to Vertica with
        ``vertica_python.vertica.connection.Connection``.
    name : str
        Name of the table into which the data will be inserted.
    schema : str (default = "public")
        Schema in which the table exists
    relation_type : str (default = "insert")
        How the dataframe should be uploaded. If the table does not yet
        exist, pass "table". If the data should be inserted into an
        existing table, pass "insert"
    parse_n_lines : optional int
        This parameter is passed directly to
        ``verticapy.utilities.pandas_to_vertica``.

    """

    if not parse_n_lines:
        parse_n_lines = df.shape[0]

    already_exists = _check_if_exists(connection_kwargs, name, schema=schema)

    logging.debug(f"daskdf_to_vertica: {schema=} | {name=}")
    logging.debug(f"daskdf_to_vertica: {already_exists=}")
    logging.debug(f"daskdf_to_vertica: {relation_type=}")

    with SerializableLock(token="daskdf_to_vertica"):
        with connect(**connection_kwargs) as connection:
            with connection.cursor() as cursor:
                tmp = f"tmp_{name}_{uuid.uuid4().hex}"

                logging.debug(f"daskdf_to_vertica: converting to vdf with {tmp=}")

                vdf = pandas_to_vertica(
                    df,
                    cursor=cursor,
                    name=tmp,
                    schema=schema,
                    parse_n_lines=parse_n_lines,
                )
                vdf.to_db(
                    f'"{schema}"."{name}"',
                    df.columns.tolist(),
                    relation_type=relation_type,
                    inplace=True,
                )


def _check_if_exists(
    connection_kwargs: Dict, name: str, schema: str = "public"
) -> bool:
    """Quick check if a Vertica table already exists in a schema

    Parameters
    ----------
    connection_kwargs : dict
        Connection arguments used when connecting to Vertica with
        ``vertica_python.vertica.connection.Connection``.
    name : str
        Name of the table to check.
    schema : str (default = "public")
        Schema in which to check for the table

    Returns
    -------
    exists : bool

    """

    with connect(**connection_kwargs) as connection:
        with connection.cursor() as cur:
            table_query = f"""
                SELECT TABLE_SCHEMA, TABLE_NAME FROM V_CATALOG.TABLES
                WHERE TABLE_SCHEMA = '{schema}'
            """

            all_vtables = readSQL(table_query, cursor=cur)
            does_exist = name in all_vtables["TABLE_NAME"]

            logging.debug(f"_check_if_exists: {all_vtables=}")
            logging.debug(f"_check_if_exists: {schema=} | {name=}")

    logging.debug(f"{(schema, name, does_exist)}")
    return does_exist


def _drop_table(connection: Connection, name: str, schema: str = "public") -> None:
    """Little helper to drop a Vertica table with common parameters"""

    logging.debug(f"_drop_table: {schema=} | {name=}")
    with connection.cursor() as cur:
        drop(name=f'"{schema}"."{name}"', cursor=cur)


def _validate_daskdf(df: Union[vDataFrame, pd.DataFrame, dd.DataFrame]) -> dd.DataFrame:
    if isinstance(df, vDataFrame):
        df = df.to_pandas()

    if isinstance(df, pd.DataFrame):
        df = dd.from_pandas(df)

    return df


def to_vertica(
    df: Union[vDataFrame, pd.DataFrame, dd.DataFrame],
    connection_kwargs: Dict,
    name: str,
    schema: str = "public",
    if_exists: str = "error",
):
    """Write a Dask DataFrame to a Vertica table.

    Parameters
    ----------
    df : dask.DataFrame
        Dask DataFrame to save.
    connection_kwargs : dict
        Connection arguments used when connecting to Vertica with
        ``vertica_python.vertica.connection.Connection``.
    name : str
        Name of the table to save to.
    schema : str (default = "public")
        Schema in which the table exists
    if_exists : str (default = "error")
        How dask_vertica should handle attempts to write to an existing
        table. Options are:

        - "error" (default): raise a Runtime error if the table exists
        - "overwrite": drop the existing table and start fresh
        - "insert": keep the existing able and insert/append new rows

    Examples
    --------

    >>> from dask_vertica import to_vertica
    >>> df = ...  # Create a Dask DataFrame
    >>> to_vertica(
    ...     df,
    ...     connection_kwargs={
    ...         "host": "...",
    ...         "port": 5433,
    ...         "database": "...",
    ...         "user": "...",
    ...         "password": "...",
    ...     },
    ...     name="my_table",
    ...     schema="project_schema",
    ...     if_exists="overwrite",
    ... )

    """

    df = _validate_daskdf(df)

    if_exists = if_exists.lower()
    table_already_exists = _check_if_exists(connection_kwargs, name, schema=schema)

    logging.debug(
        f"to_vertica: {schema=} | {name=} | {if_exists=} | {table_already_exists=} ({df.npartitions=})"
    )

    if table_already_exists:
        if if_exists == "error":
            raise RuntimeError(f"Table {name} already exists in {schema}")
        elif if_exists == "overwrite":
            with connect(**connection_kwargs) as connection:
                _drop_table(connection, name, schema=schema)

    first_relation = "insert" if if_exists in ("append", "insert") else "table"

    futures = []
    for n, partition in enumerate(df.to_delayed()):
        f = daskdf_to_vertica(
            partition,
            connection_kwargs,
            name,
            relation_type=first_relation if n == 0 else "insert",
            schema=schema,
        )

        # HACK: need to get the first partition computed to create the table
        # (empty tables -- i.e., df._meta -- raise errors)
        if n == 0:
            dask.compute(f)
        else:
            futures.append(f)

    dask.compute(futures)
    return None


@delayed
def _fetch_vdf_batch(
    connection_kwargs: Dict,
    name: str,
    partition_size: int,
    offset: int,
    schema: str = "public",
):
    with connect(**connection_kwargs) as connection:
        with connection.cursor() as cur:
            vdf = vDataFrame(name, cur, schema=schema).iloc(
                limit=partition_size, offset=offset
            )

            return vdf.to_pandas()


def read_vertica(
    connection_kwargs: Dict,
    name: str,
    npartitions: int,
    schema: str = "public",
):
    """Fetch data from a Vertica table

    Parameters
    ----------
    connection_kwargs : dict
        Connection arguments used when connecting to Vertica with
        ``vertica_python.vertica.connection.Connection``.
    name : str
        Name of the table to check.
    npartitions : int
        The number of partitions of the resulting dask dataframe
    schema : str (default = "public")
        Schema in which to check for the table

    Returns
    -------
    ddf : dd.DataFrame

    Examples
    --------

    >>> from dask_vertica import read_vertica
    >>> read_vertica(
    ...     df,
    ...     connection_kwargs={
    ...         "host": "...",
    ...         "port": 5433,
    ...         "database": "...",
    ...         "user": "...",
    ...         "password": "...",
    ...     },
    ...     "my_table",
    ...     52,
    ...     schema="project_schema",
    ... )

    """

    label = "read-vertica-"
    output_name = label + tokenize(connection_kwargs, name, npartitions, schema)

    with connect(**connection_kwargs) as connection:
        with connection.cursor() as cur:
            vdf = vDataFrame(name, cur, schema=schema)
            n_rows = vdf.shape()[0]
            meta = vdf.head(1).to_pandas().drop(0)

    partitionsize = n_rows // npartitions

    _fetcher = partial(
        _fetch_vdf_batch, connection_kwargs, name, int(partitionsize), schema=schema
    )
    offsets = np.arange(0, n_rows, partitionsize, dtype=int)
    batches = [_fetcher(int(offset)) for offset in offsets]

    if not batches:
        divisions = (None, None)
    else:
        divisions = tuple([None] * (len(batches) + 1))

    dd_obj = dd.from_delayed(
        batches, meta=meta, divisions=divisions, verify_meta=True, prefix=output_name
    )
    return dd_obj
