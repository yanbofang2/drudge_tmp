"""Local computation compatibility layer to replace Dask functionality.

This module provides simple local replacements for the distributed functionality.
"""

import functools
from typing import Any, Callable, Iterable, List, Optional, Union


class LocalBag:
    """Local wrapper to provide RDD-like interface."""
    
    def __init__(self, data: List):
        """Initialize with a list of data."""
        self._data = list(data) if data is not None else []
    
    @property
    def context(self):
        """Get the context (returns self for compatibility)."""
        return self
    
    def map(self, func: Callable) -> 'LocalBag':
        """Apply function to each element."""
        return LocalBag([func(item) for item in self._data])
    
    def flatMap(self, func: Callable) -> 'LocalBag':
        """Apply function to each element and flatten results."""
        result = []
        for item in self._data:
            mapped = func(item)
            if hasattr(mapped, '__iter__') and not isinstance(mapped, (str, bytes)):
                result.extend(mapped)
            else:
                result.append(mapped)
        return LocalBag(result)
    
    def filter(self, func: Callable) -> 'LocalBag':
        """Filter elements using function."""
        return LocalBag([item for item in self._data if func(item)])
    
    def collect(self) -> List:
        """Collect all elements to a list."""
        return list(self._data)
    
    def count(self) -> int:
        """Count the number of elements."""
        return len(self._data)
    
    def cache(self) -> 'LocalBag':
        """Cache the bag (no-op for local)."""
        return self
    
    def repartition(self, num_partitions: int) -> 'LocalBag':
        """Repartition the bag (no-op for local)."""
        return self
    
    def aggregate(self, zero_value, seq_func, comb_func):
        """Aggregate using sequence and combine functions."""
        if not self._data:
            return zero_value
        result = zero_value
        for item in self._data:
            result = seq_func(result, item)
        return result
    
    def reduce(self, func: Callable):
        """Reduce using function."""
        if not self._data:
            raise ValueError("Cannot reduce empty bag")
        result = self._data[0]
        for item in self._data[1:]:
            result = func(result, item)
        return result
    
    def reduceByKey(self, func: Callable) -> 'LocalBag':
        """Reduce by key operation."""
        # Group by key
        grouped = {}
        for item in self._data:
            key, value = item
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(value)
        
        # Reduce each group
        result = []
        for key, values in grouped.items():
            if values:
                reduced_value = values[0]
                for value in values[1:]:
                    reduced_value = func(reduced_value, value)
                result.append((key, reduced_value))
        
        return LocalBag(result)
    
    def union(self, *other_bags) -> 'LocalBag':
        """Union with other bags."""
        result = list(self._data)
        
        for bag in other_bags:
            if hasattr(bag, '_data'):
                result.extend(bag._data)
            elif hasattr(bag, '__iter__'):
                result.extend(bag)
            else:
                result.append(bag)
        
        return LocalBag(result)
    
    def cartesian(self, other_bag) -> 'LocalBag':
        """Cartesian product with another bag."""
        other_data = other_bag._data if hasattr(other_bag, '_data') else list(other_bag)
        result = []
        for item1 in self._data:
            for item2 in other_data:
                result.append((item1, item2))
        return LocalBag(result)
    
    def countByKey(self) -> dict:
        """Count occurrences of each key."""
        result = {}
        for item in self._data:
            if isinstance(item, (tuple, list)) and len(item) >= 2:
                key = item[0]
            else:
                key = item
            result[key] = result.get(key, 0) + 1
        return result
        
    def keys(self) -> 'LocalBag':
        """Get the keys from (key, value) pairs."""
        return LocalBag([item[0] for item in self._data])
        
    def values(self) -> 'LocalBag':
        """Get the values from (key, value) pairs."""
        return LocalBag([item[1] for item in self._data])
        
    def sortBy(self, key_func) -> 'LocalBag':
        """Sort the bag by a key function."""
        sorted_data = sorted(self._data, key=key_func)
        return LocalBag(sorted_data)


class LocalContext:
    """Local context to replace distributed computation context."""
    
    def __init__(self):
        """Initialize the local context."""
        pass
    
    @property
    def defaultParallelism(self) -> int:
        """Get default parallelism (always 1 for local)."""
        return 1
    
    def parallelize(self, data: Iterable, num_partitions: Optional[int] = None) -> LocalBag:
        """Create a bag from an iterable."""
        return LocalBag(data)
    
    def broadcast(self, value: Any) -> 'LocalBroadcast':
        """Create a broadcast variable."""
        return LocalBroadcast(value)
    
    def union(self, *bags) -> LocalBag:
        """Union multiple bags."""
        if not bags:
            return LocalBag([])
        
        # Handle both list of bags and individual bags as arguments
        if len(bags) == 1 and hasattr(bags[0], '__iter__') and not hasattr(bags[0], '_data'):
            # Single argument that's a list of bags
            bags = bags[0]
        
        result = []
        for bag in bags:
            if hasattr(bag, '_data'):
                result.extend(bag._data)
            elif hasattr(bag, '__iter__'):
                result.extend(bag)
            else:
                result.append(bag)
        
        return LocalBag(result)


class LocalBroadcast:
    """Local broadcast variable."""
    
    def __init__(self, value: Any):
        """Initialize broadcast variable."""
        self._value = value
    
    @property
    def value(self):
        """Get the broadcast value."""
        return self._value


class LocalVariable:
    """Local variable to replace BCastVar functionality."""
    
    def __init__(self, context: LocalContext, var: Any):
        """Initialize the variable."""
        self._context = context
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
            self._bcast = self._context.broadcast(self._var)
        return self._bcast


def nest_bind_local(bag: LocalBag, func: Callable, full_balance: bool = True) -> LocalBag:
    """Local version of nest_bind utility function.
    
    When an entry no longer need processing, None can be returned by the 
    callback function.
    """
    if full_balance:
        return _nest_bind_full_balance_local(bag, func)
    else:
        return _nest_bind_no_balance_local(bag, func)


def _nest_bind_full_balance_local(bag: LocalBag, func: Callable) -> LocalBag:
    """Nest the flat map of the given function with full load balancing."""
    
    def wrapped(obj):
        """Wrapped function for nest bind."""
        vals = func(obj)
        if vals is None:
            return [(False, obj)]
        else:
            return [(True, i) for i in vals]
    
    curr = bag
    result_items = []
    
    while curr.count() > 0:
        step_res = curr.flatMap(wrapped)
        
        new_entries = step_res.filter(lambda x: not x[0]).map(lambda x: x[1])
        result_items.extend(new_entries.collect())
        
        curr = step_res.filter(lambda x: x[0]).map(lambda x: x[1])
    
    return LocalBag(result_items)


def _nest_bind_no_balance_local(bag: LocalBag, func: Callable) -> LocalBag:
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