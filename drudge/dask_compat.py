"""Dask compatibility layer for Spark functionality.

This module provides Dask equivalents for Spark operations used in the drudge
library, allowing seamless migration from Spark to Dask.
"""

import functools
from typing import Any, Callable, Iterable, List, Optional, Union

try:
    import dask
    import dask.bag as db
    HAS_DASK = True
except ImportError:
    HAS_DASK = False

try:
    from dask.distributed import Client, Variable
    HAS_DISTRIBUTED = True
except ImportError:
    HAS_DISTRIBUTED = False

if not HAS_DASK:
    raise ImportError(
        "Dask is required for this functionality. "
        "Please install dask with: pip install 'dask[bag]'"
    )


class DaskBag:
    """Wrapper around Dask Bag to provide RDD-like interface."""

    def __init__(self, bag: db.Bag):
        """Initialize with a Dask Bag."""
        self._bag = bag

    @property
    def bag(self) -> db.Bag:
        """Get the underlying Dask Bag."""
        return self._bag

    def map(self, func: Callable) -> 'DaskBag':
        """Apply a function to each element."""
        return DaskBag(self._bag.map(func))

    def flatMap(self, func: Callable) -> 'DaskBag':
        """Apply a function and flatten the result."""
        return DaskBag(self._bag.map(func).flatten())

    def filter(self, func: Callable) -> 'DaskBag':
        """Filter elements based on a predicate."""
        return DaskBag(self._bag.filter(func))

    def collect(self) -> List[Any]:
        """Collect all elements to the driver."""
        return self._bag.compute()

    def compute(self) -> List[Any]:
        """Compute all elements (Dask native method)."""
        return self._bag.compute()

    def count(self) -> int:
        """Count the number of elements."""
        return self._bag.count().compute()

    def cache(self) -> 'DaskBag':
        """Cache the bag (persist in Dask)."""
        self._bag = self._bag.persist()
        return self

    def repartition(self, num_partitions: int) -> 'DaskBag':
        """Repartition the bag."""
        return DaskBag(self._bag.repartition(npartitions=num_partitions))

    def reduceByKey(self, func: Callable) -> 'DaskBag':
        """Reduce by key operation (assumes elements are (key, value) pairs)."""
        # Group by key and then reduce values for each key
        grouped = self._bag.groupby(lambda x: x[0])
        
        def reduce_values(key_and_values):
            key, values = key_and_values
            values_list = list(values)
            if len(values_list) == 0:
                return None
            elif len(values_list) == 1:
                return values_list[0]
            else:
                # Extract values (second element of each pair)
                vals = [v[1] for v in values_list]
                result = vals[0]
                for val in vals[1:]:
                    result = func(result, val)
                return (key, result)
        
        reduced = grouped.map(reduce_values).filter(lambda x: x is not None)
        return DaskBag(reduced)

    def reduce(self, func: Callable) -> Any:
        """Reduce the bag using the given function."""
        # For compatibility with Spark RDD.reduce(), we need to implement
        # a true reduce operation. Dask doesn't have this directly.
        items = self._bag.compute()
        if not items:
            raise ValueError("Cannot reduce empty bag")
        
        result = items[0]
        for item in items[1:]:
            result = func(result, item)
        return result

    @property
    def context(self) -> 'DaskContext':
        """Get the context (for compatibility)."""
        return DaskContext.get_current()


class DaskContext:
    """Dask context that mimics SparkContext interface."""

    _current_context = None

    def __init__(self, client: Optional['Client'] = None):
        """Initialize the Dask context."""
        if HAS_DISTRIBUTED and client is None:
            # Try to get existing client or create a new one
            try:
                client = Client.current()
            except (ValueError, ImportError):
                # No client exists, create a new one
                try:
                    client = Client(processes=False, silence_logs=False)
                except Exception:
                    # If we can't create a distributed client, use synchronous mode
                    client = None
        
        self._client = client
        # For basic bag operations, we don't need distributed
        # Set a reasonable default parallelism
        if self._client and hasattr(self._client, 'ncores'):
            self._default_parallelism = self._client.ncores()
        else:
            # Use number of CPU cores as default
            import os
            self._default_parallelism = os.cpu_count() or 4
        DaskContext._current_context = self

    @property
    def client(self) -> Optional['Client']:
        """Get the Dask client."""
        return self._client

    @property
    def defaultParallelism(self) -> int:
        """Get the default parallelism."""
        return self._default_parallelism

    def parallelize(self, data: Iterable, numSlices: Optional[int] = None) -> DaskBag:
        """Create a DaskBag from a collection."""
        if numSlices is None:
            numSlices = self._default_parallelism
        bag = db.from_sequence(data, npartitions=numSlices)
        return DaskBag(bag)

    def broadcast(self, value: Any) -> 'DaskBroadcast':
        """Broadcast a variable."""
        return DaskBroadcast(value, self._client)

    def union(self, bags: List[DaskBag]) -> DaskBag:
        """Union multiple bags."""
        if not bags:
            # Return empty bag
            return DaskBag(db.from_sequence([], npartitions=1))
        
        dask_bags = [bag.bag for bag in bags]
        union_bag = db.concat(dask_bags)
        return DaskBag(union_bag)

    @classmethod
    def get_current(cls) -> 'DaskContext':
        """Get the current context."""
        if cls._current_context is None:
            cls._current_context = DaskContext()
        return cls._current_context

    def stop(self):
        """Stop the context."""
        if self._client and hasattr(self._client, 'close'):
            self._client.close()
        DaskContext._current_context = None


class DaskBroadcast:
    """Dask broadcast variable that mimics Spark broadcast."""

    def __init__(self, value: Any, client: Optional['Client']):
        """Initialize the broadcast variable."""
        self._value = value
        self._client = client
        # In Dask, we can use Variables for shared state
        # but for simplicity, we'll just store the value locally
        # since Dask handles serialization efficiently

    @property
    def value(self) -> Any:
        """Get the broadcast value."""
        return self._value


# Compatibility aliases
SparkContext = DaskContext
RDD = DaskBag


def create_context(**kwargs) -> DaskContext:
    """Create a new Dask context."""
    return DaskContext()