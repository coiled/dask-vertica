# Dask-vertica

[![Tests](https://github.com/coiled/dask-vertica/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/dask-vertica/actions/workflows/tests.yml)
[![Linting](https://github.com/coiled/dask-vertica/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/coiled/dask-vertica/actions/workflows/pre-commit.yml)


This connector is in an early experimental/testing phase.

[Reach out to us](https://coiled.io/contact-us/) if you are interested in trying
it out!

## Installation

`dask-vertica` can be installed from source:

```zsh
git clone git@github.com:coiled/dask-vertica.git
cd dask-vertica
pip install -e .
```

<!--
`dask-vertica` can be installed with `pip`:

```
pip install dask-vertica
```

or with `conda`:

```
conda install -c conda-forge dask-vertica
```
-->

## Usage

`dask-vertica` provides `read_vertica` and `to_vertica` methods
for parallel IO from vertica with Dask.

```python
>>> from dask_vertica import read_vertica
>>> example_query = '''
...    SELECT *
...    FROM VERTICA_SAMPLE_DATA.TPCH_SF1.CUSTOMER;
... '''
>>> ddf = read_vertica(
...     query=example_query,
...     connection_kwargs={
...         "user": "...",
...         "password": "...",
...         ...
...     },
... )
```

`read_vertica` will return a dask dataframe, so data will not be fetched until `.compute()` or `.persist()` is called.
This means that you can lazily perform calculations and upload the results at the same time.
Note that `to_vertica` calls `.compute()` for you, so it doesn't appear in the example below.

```python
>>> from dask_vertica import to_vertica
>>> means = ddf.groupby("name")["y"].mean() # lazy, no calc performed
>>> to_vertica(
...     means,
...     name="my_table",
...     connection_kwargs={
...         "user": "...",
...         "password": "...",
...         ...,
...     },
... ) # computes means and uploads to DB
```

See their docstrings for further API information.

## License

[BSD-3](LICENSE)
