"""Configuration for particle-hole problem with explicit spin.
"""

from drudge.dask_compat import create_context
from drudge import RestrictedPartHoleDrudge

ctx = create_context()
dr = RestrictedPartHoleDrudge(ctx)
dr.full_simplify = False

DRUDGE = dr
