"""Project-wide shared test fixtures."""

import os

import pytest

# Keep environment variable check for backward compatibility during migration
IF_DUMMY_SPARK = 'DUMMY_SPARK' in os.environ

@pytest.fixture(scope='session', autouse=True)
def dask_ctx():
    """A simple dask context."""
    from drudge.dask_compat import DaskContext
    return DaskContext()


def skip_in_spark(**kwargs):
    """Skip the test in Apache Spark environment.

    Mostly due to issues with pickling some SymPy objects, some tests have to
    be temporarily skipped in Apache Spark environment.
    """
    # Since we're now using Dask, we can enable all tests
    return pytest.mark.skipif(False, **kwargs)
