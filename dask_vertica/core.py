import logging
import uuid
from typing import Dict, Optional

from vertica_python import connect
from vertica_python.vertica.connection import Connection
from verticapy.utilities import drop, pandas_to_vertica, readSQL

import dask
import dask.dataframe as dd
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

                logging.debug(f"daskdf_to_vertica: {tmp=}")
                logging.debug(f"daskdf_to_vertica: converting to vdf")

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


def to_vertica(
    df: dd.DataFrame,
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
    name : str
        Name of the table to save to.
    connection_kwargs : dict
        Connection arguments used when connecting to Vertica with
        ``vertica_python.vertica.connection.Connection``.
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
    ...         "user": "...",
    ...         "password": "...",
    ...         ...
    ...     },
    ...     name="my_table",
    ...     schema="project_schema",
    ...     if_exists="overwrite",
    ... )

    """
    if_exists = if_exists.lower()
    table_already_exists = _check_if_exists(connection_kwargs, name, schema=schema)

    logging.debug(
        f"to_vertica: {schema=} | {name=} | {if_exists=} | {table_already_exists=}"
    )

    if table_already_exists:
        if if_exists == "error":
            raise RuntimeError(f"Table {name} already exists in {schema}")
        elif if_exists == "overwrite":
            with connect(**connection_kwargs) as connection:
                _drop_table(connection, name, schema=schema)

    futures = []
    for n, partition in enumerate(df.to_delayed()):
        f = daskdf_to_vertica(
            partition,
            connection_kwargs,
            name,
            relation_type="table" if n == 0 else "insert",
            schema=schema,
        )

        # HACK: need to get the first partition computed to create the table
        # (empty tables -- i.e., df._meta -- raise errors)
        if n == 0:
            dask.compute(f)
        else:
            futures.append(f)

    return dask.compute(futures)


def read_vertica(*args, **kwargs):
    raise NotImplementedError
