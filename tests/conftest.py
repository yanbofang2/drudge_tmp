"""Project-wide shared test fixtures."""

import pytest

# No longer need context fixtures since Drudge doesn't require them


def skip_in_spark(**kwargs):
    """Skip the test in Apache Spark environment.

    Deprecated: No longer relevant since we're not using Spark.
    Keeping for backward compatibility but always returns False.
    """
    return pytest.mark.skipif(False, **kwargs)
