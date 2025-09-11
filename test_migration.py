#!/usr/bin/env python
"""
Minimal test to validate Spark to Dask migration.
This test script bypasses the full package import to test core functionality.
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

def test_dask_compatibility():
    """Test basic Dask compatibility layer functionality."""
    print("Testing Dask compatibility layer...")
    
    # Import and test the compatibility layer
    from drudge.dask_compat import DaskContext, DaskBag
    
    # Test context creation
    ctx = DaskContext()
    print(f"✓ Created DaskContext with {ctx.defaultParallelism} default parallelism")
    
    # Test basic operations
    data = [1, 2, 3, 4, 5]
    bag = ctx.parallelize(data)
    print(f"✓ Parallelized data: {data}")
    
    # Test map operation
    mapped = bag.map(lambda x: x * 2)
    result = mapped.collect()
    expected = [x * 2 for x in data]
    assert result == expected, f"Expected {expected}, got {result}"
    print(f"✓ Map operation successful: {result}")
    
    # Test filter operation
    filtered = bag.filter(lambda x: x % 2 == 0)
    result = filtered.collect()
    expected = [x for x in data if x % 2 == 0]
    assert result == expected, f"Expected {expected}, got {result}"
    print(f"✓ Filter operation successful: {result}")
    
    # Test flatMap operation
    flat_mapped = bag.flatMap(lambda x: [x, x])
    result = flat_mapped.collect()
    expected = []
    for x in data:
        expected.extend([x, x])
    assert result == expected, f"Expected {expected}, got {result}"
    print(f"✓ FlatMap operation successful: {result}")
    
    # Test count operation
    count = bag.count()
    assert count == len(data), f"Expected {len(data)}, got {count}"
    print(f"✓ Count operation successful: {count}")
    
    # Test broadcast
    broadcast_var = ctx.broadcast({'config': 'value'})
    assert broadcast_var.value == {'config': 'value'}
    print(f"✓ Broadcast successful: {broadcast_var.value}")
    
    # Test union
    bag2 = ctx.parallelize([6, 7, 8])
    union_bag = ctx.union([bag, bag2])
    result = union_bag.collect()
    expected = data + [6, 7, 8]
    # Note: order might not be preserved in union
    assert set(result) == set(expected), f"Expected {expected}, got {result}"
    print(f"✓ Union operation successful: {sorted(result)}")
    
    # Test cache and repartition
    cached = bag.cache()
    repartitioned = cached.repartition(2)
    result = repartitioned.collect()
    assert set(result) == set(data), f"Expected {data}, got {result}"
    print(f"✓ Cache and repartition successful: {sorted(result)}")
    
    print("✓ All Dask compatibility tests passed!")

def test_imports():
    """Test that critical imports work."""
    print("\nTesting critical imports...")
    
    try:
        import dask.bag as db
        print("✓ dask.bag imported successfully")
    except ImportError as e:
        print(f"✗ dask.bag import failed: {e}")
        return False
    
    try:
        from drudge.dask_compat import DaskContext, DaskBag, DaskBroadcast
        print("✓ dask_compat classes imported successfully")
    except ImportError as e:
        print(f"✗ dask_compat import failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Starting Spark to Dask migration validation tests...\n")
    
    if not test_imports():
        sys.exit(1)
    
    try:
        test_dask_compatibility()
        print("\n🎉 All tests passed! Spark to Dask migration basic functionality is working.")
    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)