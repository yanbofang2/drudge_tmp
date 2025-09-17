"""Configures a simple drudge for particle-hole model."""

from drudge import DaskContext, PartHoleDrudge

ctx = DaskContext()
dr = PartHoleDrudge(ctx)
dr.full_simplify = False

DRUDGE = dr
