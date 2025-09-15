"""Project-wide shared test fixtures."""

import os

import pytest

# Use environment variable to control between local and distributed Dask
USE_DISTRIBUTED_DASK = 'DISTRIBUTED_DASK' in os.environ

@pytest.fixture(scope='session', autouse=True)
def local_ctx():
    """A local context for testing."""
    from drudge.local_compat import LocalContext
    
    # Always use local computation
    ctx = LocalContext()
    
    return ctx


def skip_in_distributed(**kwargs):
    """Skip the test in distributed Dask environment.
    
    Some tests may need to be skipped in distributed mode due to 
    serialization or other issues.
    """
    return pytest.mark.skipif(USE_DISTRIBUTED_DASK, **kwargs)
