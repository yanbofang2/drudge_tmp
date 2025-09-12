"""
Example demonstrating the migration from Spark to Dask in Drudge.

This shows how user code should be updated to use the new Dask backend.
"""

# Before migration (Spark):
# from pyspark import SparkContext, SparkConf
# conf = SparkConf().setMaster('local[2]').setAppName('drudge-example')
# ctx = SparkContext(conf=conf)

# After migration (Dask):
from drudge import DaskContext, Drudge

# For local computation (similar to dummy_spark):
ctx = DaskContext()

# For distributed computation:
# from dask.distributed import Client
# client = Client('scheduler-address:8786')  # or Client() for local cluster
# ctx = DaskContext(client)

# Create a drudge instance
dr = Drudge(ctx)

# The rest of the API remains the same!
# All tensor operations work as before:
# tensor1 = dr.sum(...)
# result = tensor1.expand().simplify()
# etc.

print("Drudge with Dask backend initialized successfully!")
print(f"Default parallelism: {ctx.defaultParallelism}")