"""Dask compatibility layer for Spark RDD operations.

This module provides Dask-based replacements for Spark RDD functionality used in Drudge.
"""

import functools
from typing import Any, Callable, Iterable, List, Optional, Set, Union

import dask
import dask.bag as db
from dask.distributed import Client, Variable


class DaskBag:
    """Wrapper around Dask Bag to provide RDD-like interface."""
    
    def __init__(self, bag: db.Bag):
        """Initialize with a Dask bag."""
        self._bag = bag
    
    @property
    def bag(self):
        """Get the underlying Dask bag."""
        return self._bag
    
    def map(self, func: Callable) -> 'DaskBag':
        """Map function over bag elements."""
        return DaskBag(self._bag.map(func))
    
    def flatMap(self, func: Callable) -> 'DaskBag':
        """Flat map function over bag elements."""
        return DaskBag(self._bag.map(func).flatten())
    
    def filter(self, func: Callable) -> 'DaskBag':
        """Filter bag elements."""
        return DaskBag(self._bag.filter(func))
    
    def reduce(self, func: Callable) -> Any:
        """Reduce bag to single value."""
        return self._bag.fold(func)
    
    def collect(self) -> List[Any]:
        """Collect all bag elements to a list."""
        return self._bag.compute()
    
    def count(self) -> int:
        """Count number of elements."""
        return self._bag.count().compute()
    
    def take(self, num: int) -> List[Any]:
        """Take first num elements."""
        return self._bag.take(num)
    
    def union(self, other: 'DaskBag') -> 'DaskBag':
        """Union with another bag."""
        return DaskBag(db.concat([self._bag, other._bag]))
    
    def cartesian(self, other: 'DaskBag') -> 'DaskBag':
        """Cartesian product with another bag."""
        def cart_product(x):
            return [(x, y) for y in other.collect()]
        return DaskBag(self._bag.map(cart_product).flatten())
    
    def groupByKey(self) -> 'DaskBag':
        """Group by key (assumes elements are (key, value) pairs)."""
        return DaskBag(self._bag.groupby(lambda x: x[0]))
    
    def distinct(self) -> 'DaskBag':
        """Get distinct elements."""
        return DaskBag(self._bag.distinct())
    
    def repartition(self, num_partitions: int) -> 'DaskBag':
        """Repartition the bag."""
        return DaskBag(self._bag.repartition(num_partitions))
    
    def getNumPartitions(self) -> int:
        """Get number of partitions."""
        return self._bag.npartitions
    
    def reduceByKey(self, func: Callable) -> 'DaskBag':
        """Reduce by key (assumes elements are (key, value) pairs)."""
        # Use dask's foldby which is more efficient than groupby
        def combine_func(acc, value):
            if acc is None:
                return value
            return func(acc, value)
        
        def reduce_func(x, y):
            return func(x, y)
        
        # Use foldby to group and reduce
        result_bag = self._bag.foldby(
            key=lambda x: x[0],  # group by key
            binop=lambda acc, item: combine_func(acc, item[1]),  # combine function
            initial=None,  # initial value
            combine=reduce_func  # final combine
        )
        
        # Filter out None values and return proper key-value pairs
        return DaskBag(result_bag.filter(lambda x: x[1] is not None))
    
    def aggregate(self, zero_value: Any, seq_func: Callable, comb_func: Callable) -> Any:
        """Aggregate the bag using sequential and combine functions."""
        # Map each element, then fold to combine results
        # First apply the seq_func to combine each element with zero_value
        mapped = self._bag.map(lambda x: seq_func(zero_value.__class__(), x))
        # Then fold to combine all the results
        return mapped.fold(zero_value, comb_func).compute()
    
    def cache(self) -> 'DaskBag':
        """Cache the bag (no-op in Dask, returns self)."""
        return self
    
    def unpersist(self) -> 'DaskBag':
        """Unpersist the bag (no-op in Dask, returns self)."""
        return self


class DaskContext:
    """Dask context to replace SparkContext."""
    
    def __init__(self, client: Optional[Client] = None):
        """Initialize the Dask context."""
        if client is None:
            # Use synchronous local scheduler to avoid serialization issues
            import dask
            self._client = None
            # Set global dask config to use synchronous scheduler
            dask.config.set(scheduler='synchronous')
        else:
            self._client = client
    
    @property
    def client(self):
        """Get the Dask client."""
        return self._client
    
    @property
    def defaultParallelism(self) -> int:
        """Get default parallelism (number of workers * threads per worker)."""
        if self._client is None:
            return 1  # Local mode
        try:
            info = self._client.scheduler_info()
            total_cores = sum(worker['nthreads'] for worker in info['workers'].values())
            return max(1, total_cores)
        except:
            return 4  # Default fallback
    
    def parallelize(self, data: Iterable, num_partitions: Optional[int] = None) -> DaskBag:
        """Create a bag from an iterable."""
        if num_partitions is None:
            num_partitions = self.defaultParallelism
        bag = db.from_sequence(data, npartitions=num_partitions)
        return DaskBag(bag)
    
    def broadcast(self, value: Any) -> 'DaskBroadcast':
        """Create a broadcast variable."""
        if self._client is None:
            # Local mode - return a simple wrapper
            return DaskBroadcast(None, value)
        return DaskBroadcast(self._client, value)
    
    def union(self, *bags) -> DaskBag:
        """Union multiple bags."""
        dask_bags = []
        for bag in bags:
            if isinstance(bag, DaskBag):
                dask_bags.append(bag.bag)
            else:
                # Assume it's already a dask bag
                dask_bags.append(bag)
        return DaskBag(db.concat(dask_bags))


class DaskBroadcast:
    """Dask broadcast variable using distributed variables."""
    
    def __init__(self, client: Optional[Client], value: Any):
        """Initialize broadcast variable."""
        self._client = client
        self._value = value
        # Store in distributed memory if possible
        if client is not None:
            try:
                self._var = Variable(name=f"broadcast_{id(self)}", client=client)
                self._var.set(value)
            except:
                # Fallback to local storage
                self._var = None
        else:
            # Local mode - no distributed variable
            self._var = None
    
    @property
    def value(self):
        """Get the broadcast value."""
        if self._var is not None:
            try:
                return self._var.get()
            except:
                pass
        return self._value


class DaskVariable:
    """Dask variable to replace BCastVar functionality."""
    
    def __init__(self, client: DaskContext, var: Any):
        """Initialize the variable."""
        self._client = client
        self._var = var
        self._bcast = None
    
    @property
    def var(self):
        """Get the variable to mutate."""
        self._bcast = None
        return self._var
    
    @property
    def ro(self):
        """Get the variable, read-only."""
        return self._var
    
    @property
    def bcast(self):
        """Get the broadcast variable."""
        if self._bcast is None:
            self._bcast = self._client.broadcast(self._var)
        return self._bcast


def nest_bind_dask(bag: DaskBag, func: Callable, full_balance: bool = True) -> DaskBag:
    """Nest the flat map of the given function.
    
    This is the Dask equivalent of the nest_bind function for Spark RDDs.
    """
    if full_balance:
        return _nest_bind_full_balance_dask(bag, func)
    else:
        return _nest_bind_no_balance_dask(bag, func)


def _nest_bind_full_balance_dask(bag: DaskBag, func: Callable) -> DaskBag:
    """Nest the flat map of the given function with full load balancing."""
    # For Dask, we can rely on its built-in load balancing
    return bag.flatMap(func)


def _nest_bind_no_balance_dask(bag: DaskBag, func: Callable) -> DaskBag:
    """Nest the flat map of the given function without load balancing."""
    # For Dask, this is the same as the balanced version since
    # Dask handles partitioning automatically
    return bag.flatMap(func)