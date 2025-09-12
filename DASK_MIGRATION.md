# Spark to Dask Migration Guide

This document describes the migration from PySpark to Dask in Drudge.

## Overview

Drudge has been migrated from using Apache Spark to Dask for distributed computing. This migration provides several benefits:

- **Easier debugging**: Pure Python implementation integrates better with debugging tools
- **Better ecosystem integration**: Works seamlessly with scientific Python libraries
- **Simpler deployment**: No need for Java/Scala dependencies
- **Unified local/distributed execution**: Same code works on single machines and clusters

## API Changes

### Dependencies

**Before (pyproject.toml):**
```toml
dependencies = [
  "pyspark>=3.5,<3.6",
  "dummyrdd @ git+https://github.com/tschijnmo/DummyRDD.git@master"
]
```

**After:**
```toml
dependencies = [
  "dask[complete]>=2024.1.0"
]
```

### Context Creation

**Before (Spark):**
```python
from pyspark import SparkContext, SparkConf

# For production
conf = SparkConf().setMaster('local[2]').setAppName('drudge-app')
ctx = SparkContext(conf=conf)

# For debugging
from dummy_spark import SparkContext
ctx = SparkContext(master='', conf=SparkConf())
```

**After (Dask):**
```python
from drudge import DaskContext

# For local computation (similar to dummy_spark)
ctx = DaskContext()

# For distributed computation
from dask.distributed import Client
client = Client('scheduler-address:8786')
ctx = DaskContext(client)
```

### Drudge Creation

**Before and After (unchanged):**
```python
from drudge import Drudge
dr = Drudge(ctx)
```

## Testing Changes

**Before (conftest.py):**
```python
@pytest.fixture(scope='session', autouse=True)
def spark_ctx():
    if IF_DUMMY_SPARK:
        from dummy_spark import SparkContext
        ctx = SparkContext(master='', conf=SparkConf())
    else:
        from pyspark import SparkContext
        ctx = SparkContext(conf=SparkConf().setMaster('local[2]'))
    return ctx

def test_example(spark_ctx):
    dr = Drudge(spark_ctx)
```

**After:**
```python
@pytest.fixture(scope='session', autouse=True)
def dask_ctx():
    from drudge.dask_compat import DaskContext
    return DaskContext()

def test_example(dask_ctx):
    dr = Drudge(dask_ctx)
```

## Environment Variables

- **Before**: `DUMMY_SPARK=1` to use dummy_spark for debugging
- **After**: No environment variable needed; local Dask is used by default
- **New**: `DISTRIBUTED_DASK=1` to use distributed Dask in tests

## Performance Considerations

1. **Local execution**: Dask's local scheduler is similar to dummy_spark performance
2. **Distributed execution**: Dask's distributed scheduler scales similarly to Spark
3. **Memory usage**: Dask's lazy evaluation and task graphs may be more memory efficient
4. **Debugging**: Python-native stack traces make debugging much easier

## Migration Checklist

- [ ] Update `pyproject.toml` dependencies
- [ ] Replace `SparkContext` imports with `DaskContext`
- [ ] Update test fixtures from `spark_ctx` to `dask_ctx`
- [ ] Remove `DUMMY_SPARK` environment variable usage
- [ ] Test with your specific workloads
- [ ] Update any custom Spark integrations

## Backward Compatibility

The migration maintains API compatibility at the Drudge level. All tensor operations, algebraic manipulations, and normal ordering work exactly the same way. Only the context creation needs to be updated.