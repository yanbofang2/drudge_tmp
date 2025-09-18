"""Configuration for particle-hole problem with explicit spin.
"""

from drudge import DaskContext, RestrictedPartHoleDrudge

ctx = DaskContext()
dr = RestrictedPartHoleDrudge(ctx)
dr.full_simplify = False

DRUDGE = dr
