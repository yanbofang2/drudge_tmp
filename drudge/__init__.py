"""
Drudge, a symbolic system for non-commutative and tensor algebra
================================================================

"""

from .canonpy import Perm, Group
from .term import Range, Vec, Term
from .canon import IDENT, NEG, CONJ
from .drudge import Tensor, TensorDef, Drudge
from .wick import WickDrudge
from .fock import (
    CR, AN, FERMI, BOSE, FockDrudge, GenMBDrudge, PartHoleDrudge,
    UP, DOWN, SpinOneHalfGenDrudge, SpinOneHalfPartHoleDrudge,
    RestrictedPartHoleDrudge, BogoliubovDrudge
)
from .genquad import GenQuadDrudge, GenQuadLatticeDrudge
from .su2 import SU2LatticeDrudge
from .clifford import CliffordDrudge, inner_by_delta
from .bcs import ReducedBCSDrudge
from .nuclear import NuclearBogoliubovDrudge
from .report import Report, ScalarLatexPrinter
from .utils import sum_, prod_, Stopwatch, CallByIndex, InvariantIndexable

__version__ = '0.11.0'

__all__ = [
    # Canonpy.
    'Perm',
    'Group',

    # Vec.
    'Vec',

    # Term.
    'Range',
    'Term',

    # Canon.
    'IDENT',
    'NEG',
    'CONJ',

    # Drudge.
    'Tensor',
    'TensorDef',
    'Drudge',

    # Different problem-specific drudges.
    #
    # Base Wick algebra.
    'WickDrudge',

    # Many-body theories.
    'CR', 'AN', 'FERMI', 'BOSE',
    'FockDrudge',
    'GenMBDrudge', 'PartHoleDrudge',
    'UP', 'DOWN',
    'SpinOneHalfGenDrudge', 'SpinOneHalfPartHoleDrudge',
    'RestrictedPartHoleDrudge',
    'BogoliubovDrudge',

    # Other algebraic systems.
    'GenQuadDrudge',
    'GenQuadLatticeDrudge',
    'SU2LatticeDrudge',
    'CliffordDrudge',
    'ReducedBCSDrudge',
    'NuclearBogoliubovDrudge',
    'inner_by_delta',

    # Small user utilities.
    'sum_',
    'prod_',
    'Stopwatch',
    'CallByIndex',
    'InvariantIndexable',
    'Report',
    'ScalarLatexPrinter'
]
