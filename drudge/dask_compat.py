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
        self._client = None
        try:
            self._client = Client.current()
        except ValueError:
            # No active client, operations will run locally
            pass
    
    @property
    def bag(self):
        """Get the underlying Dask bag."""
        return self._bag
    
    @property
    def context(self):
        """Get the context (returns self for compatibility)."""
        return self
    
    def map(self, func: Callable) -> 'DaskBag':
        """Apply function to each element."""
        return DaskBag(self._bag.map(func))
    
    def flatMap(self, func: Callable) -> 'DaskBag':
        """Apply function to each element and flatten results."""
        return DaskBag(self._bag.map(func).flatten())
    
    def filter(self, func: Callable) -> 'DaskBag':
        """Filter elements using function."""
        return DaskBag(self._bag.filter(func))
    
    def collect(self) -> List:
        """Collect all elements to a list."""
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
        return DaskBag(self._bag.repartition(num_partitions))
    
    def aggregate(self, zero_value, seq_func, comb_func):
        """Aggregate using sequence and combine functions."""
        return self._bag.fold(seq_func, zero_value, comb_func).compute()
    
    def reduce(self, func: Callable):
        """Reduce using function."""
        return self._bag.reduction(func, func).compute()
    
    def reduceByKey(self, func: Callable) -> 'DaskBag':
        """Reduce by key operation - similar to Spark's reduceByKey."""
        # Group by key, then reduce values for each key
        def reduce_group(key_values):
            key, values = key_values
            if len(values) == 1:
                return (key, values[0])
            result = values[0]
            for value in values[1:]:
                result = func(result, value)
            return (key, result)
        
        return DaskBag(self._bag.groupby(lambda x: x[0]).map(reduce_group))
    
    def union(self, other_bags: List['DaskBag']) -> 'DaskBag':
        """Union with other bags."""
        bags = [self._bag] + [bag._bag for bag in other_bags]
        return DaskBag(db.concat(bags))


class DaskContext:
    """Dask context to replace SparkContext."""
    
    def __init__(self, client: Optional[Client] = None):
        """Initialize the Dask context."""
        if client is None:
            try:
                self._client = Client.current()
            except ValueError:
                # Start a local client if none exists
                self._client = Client(processes=False, silence_logs=False)
        else:
            self._client = client
    
    @property
    def client(self):
        """Get the Dask client."""
        return self._client
    
    @property
    def defaultParallelism(self) -> int:
        """Get default parallelism (number of workers * threads per worker)."""
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
        return DaskBroadcast(self._client, value)
    
    def union(self, bags: List[DaskBag]) -> DaskBag:
        """Union multiple bags."""
        if not bags:
            return DaskBag(db.from_sequence([]))
        
        dask_bags = [bag._bag for bag in bags]
        return DaskBag(db.concat(dask_bags))


class DaskBroadcast:
    """Dask broadcast variable using distributed variables."""
    
    def __init__(self, client: Client, value: Any):
        """Initialize broadcast variable."""
        self._client = client
        self._value = value
        # Store in distributed memory if possible
        try:
            self._var = Variable(name=f"broadcast_{id(self)}", client=client)
            self._var.set(value)
        except:
            # Fallback to local storage
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
    """Dask version of nest_bind utility function.
    
    When an entry no longer need processing, None can be returned by the 
    callback function.
    """
    if full_balance:
        return _nest_bind_full_balance_dask(bag, func)
    else:
        return _nest_bind_no_balance_dask(bag, func)


def _nest_bind_full_balance_dask(bag: DaskBag, func: Callable) -> DaskBag:
    """Nest the flat map of the given function with full load balancing."""
    
    def wrapped(obj):
        """Wrapped function for nest bind."""
        vals = func(obj)
        if vals is None:
            return [(False, obj)]
        else:
            return [(True, i) for i in vals]
    
    curr = bag
    curr.cache()
    result_bags = []
    
    while curr.count() > 0:
        step_res = curr.flatMap(wrapped)
        step_res.cache()
        
        new_entries = step_res.filter(lambda x: not x[0]).map(lambda x: x[1])
        new_entries.cache()
        result_bags.append(new_entries)
        
        curr = step_res.filter(lambda x: x[0]).map(lambda x: x[1])
        curr.cache()
    
    if not result_bags:
        return DaskBag(db.from_sequence([]))
    
    return bag.context.union(result_bags)


def _nest_bind_no_balance_dask(bag: DaskBag, func: Callable) -> DaskBag:
    """Nest the flat map of the given function without load balancing."""
    
    def wrapped(obj):
        """Wrapped function for nest bind."""
        curr = [obj]
        res = []
        while len(curr) > 0:
            new_curr = []
            for i in curr:
                vals = func(i)
                if vals is None:
                    res.append(i)
                else:
                    new_curr.extend(vals)
            curr = new_curr
        return res
    
    return bag.flatMap(wrapped)