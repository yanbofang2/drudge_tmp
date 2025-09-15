"""Project-wide shared test fixtures."""

import os

import pytest

# Use environment variable to control between local and distributed Dask
USE_DISTRIBUTED_DASK = 'DISTRIBUTED_DASK' in os.environ

@pytest.fixture(scope='session', autouse=True)
def dask_ctx():
    """A Dask context for testing."""
    from drudge.utils import DaskContext
    
    if USE_DISTRIBUTED_DASK:
        # Use distributed Dask with a local cluster
        from dask.distributed import Client
        client = Client(processes=False, silence_logs=False)
        ctx = DaskContext(client)
    else:
        # Use local Dask (similar to dummy_spark behavior)
        ctx = DaskContext()
    
    return ctx


@pytest.fixture(scope='session')
def local_ctx(dask_ctx):
    """Alias for dask_ctx for backward compatibility."""
    return dask_ctx


def skip_in_distributed(**kwargs):
    """Skip the test in distributed Dask environment.
    
    Some tests may need to be skipped in distributed mode due to 
    serialization or other issues.
    """
    return pytest.mark.skipif(USE_DISTRIBUTED_DASK, **kwargs)
