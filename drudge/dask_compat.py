"""Dask compatibility layer for Spark functionality.

This module provides Dask equivalents for Spark operations used in the drudge
library, allowing seamless migration from Spark to Dask.
"""

import functools
from typing import Any, Callable, Iterable, List, Optional, Union

try:
    import dask
    import dask.bag as db
    from dask.distributed import Client, Variable
except ImportError:
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

    def reduce(self, func: Callable) -> Any:
        """Reduce the bag using the given function."""
        return self._bag.reduce(func)

    @property
    def context(self) -> 'DaskContext':
        """Get the context (for compatibility)."""
        return DaskContext.get_current()


class DaskContext:
    """Dask context that mimics SparkContext interface."""

    _current_context = None

    def __init__(self, client: Optional[Client] = None):
        """Initialize the Dask context."""
        if client is None:
            # Try to get existing client or create a new one
            try:
                client = Client.current()
            except ValueError:
                # No client exists, create a new one
                client = Client(processes=False, silence_logs=False)
        
        self._client = client
        self._default_parallelism = self._client.ncores()
        DaskContext._current_context = self

    @property
    def client(self) -> Client:
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
        if self._client:
            self._client.close()
        DaskContext._current_context = None


class DaskBroadcast:
    """Dask broadcast variable that mimics Spark broadcast."""

    def __init__(self, value: Any, client: Client):
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