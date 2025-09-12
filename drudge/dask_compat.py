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
        # Group by key, then reduce values for each key using fold
        def get_key(item):
            return item[0]
        
        def get_value(item):
            return item[1]
        
        # Group by key and reduce each group
        grouped = self._bag.groupby(get_key)
        
        def reduce_group(group_item):
            key, values = group_item
            values_list = list(values)
            if not values_list:
                return None
            result = get_value(values_list[0])
            for item in values_list[1:]:
                result = func(result, get_value(item))
            return (key, result)
        
        return DaskBag(grouped.map(reduce_group).filter(lambda x: x is not None))
    
    def union(self, *other_bags) -> 'DaskBag':
        """Union with other bags."""
        all_bags = [self._bag] + [bag._bag if hasattr(bag, '_bag') else bag for bag in other_bags]
        return DaskBag(db.concat(all_bags))
    
    def cartesian(self, other_bag) -> 'DaskBag':
        """Cartesian product with another bag."""
        other = other_bag._bag if hasattr(other_bag, '_bag') else other_bag
        
        # Get all items from both bags
        self_items = self._bag
        other_items = other
        
        # Create cartesian product using nested map
        def cartesian_map(item):
            return other_items.map(lambda other_item: (item, other_item))
        
        result = self_items.map(cartesian_map).flatten()
        return DaskBag(result)
    
    def countByKey(self) -> dict:
        """Count occurrences of each key (assumes bag contains (key, value) pairs)."""
        def get_key(item):
            return item[0] if isinstance(item, (tuple, list)) and len(item) >= 2 else item
        
        # Group by key and count
        grouped = self._bag.groupby(get_key)
        result = grouped.map(lambda x: (x[0], len(list(x[1])))).compute()
        return dict(result)
        
    def keys(self) -> 'DaskBag':
        """Get the keys from (key, value) pairs."""
        return DaskBag(self._bag.map(lambda x: x[0]))
        
    def values(self) -> 'DaskBag':
        """Get the values from (key, value) pairs."""
        return DaskBag(self._bag.map(lambda x: x[1]))
        
    def sortBy(self, key_func) -> 'DaskBag':
        """Sort the bag by a key function."""
        # Convert to list, sort, then back to bag
        sorted_items = self._bag.map_partitions(
            lambda partition: sorted(partition, key=key_func)
        )
        return DaskBag(sorted_items)


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
        if not bags:
            return DaskBag(db.from_sequence([]))
        
        # Handle both list of bags and individual bags as arguments
        if len(bags) == 1 and hasattr(bags[0], '__iter__') and not hasattr(bags[0], '_bag'):
            # Single argument that's a list of bags
            bags = bags[0]
        
        dask_bags = []
        for bag in bags:
            if hasattr(bag, '_bag'):
                dask_bags.append(bag._bag)
            else:
                # Assume it's already a dask bag
                dask_bags.append(bag)
        
        if not dask_bags:
            return DaskBag(db.from_sequence([]))
            
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