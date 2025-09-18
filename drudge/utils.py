"""Small utilities."""

import functools
import operator
import string
import time
from collections.abc import Sequence

import dask.bag as db
from sympy import (
    sympify, Symbol, Expr, SympifyError, count_ops, default_sort_key,
    AtomicExpr, Integer, S
)
from sympy.core.assumptions import ManagedProperties
from sympy.core.sympify import CantSympify


#
# SymPy utilities
# ---------------
#

def ensure_sympify(obj, role='', expected_type=None):
    """Sympify the given object with checking and error reporting.

    This is a shallow wrapper over SymPy sympify function to have error
    reporting in consistent style and an optional type checking.
    """

    header = 'Invalid {}: '.format(role)

    try:
        sympified = sympify(obj)
    except SympifyError as exc:
        raise TypeError(header, obj, 'failed to be simpified', exc.args)

    if expected_type is None or isinstance(sympified, expected_type):
        return sympified
    else:
        raise TypeError(header, sympified, 'expecting', expected_type)


def ensure_symb(obj, role=''):
    """Sympify the given object into a symbol."""
    return ensure_sympify(obj, role, Symbol)


def ensure_expr(obj, role=''):
    """Sympify the given object into an expression."""
    return ensure_sympify(obj, role, Expr)


def sympy_key(expr):
    """Get the key for ordering SymPy expressions.

    This function assumes that the given expression is already sympified.
    """

    return count_ops(expr), default_sort_key(expr)


def is_higher(obj, priority):
    """Test if the object has higher operation priority.

    When the given object does not have defined priority, it is considered
    lower.
    """

    return getattr(obj, '_op_priority', priority - 1) > priority


class NonsympifiableFunc(CantSympify):
    """Utility for wrapping callable to be used for SymPy.

    Inside SymPy functions like replace, things will first be attempted to be
    sympified, which can be very expensive.  By wrapping callable inside this
    class, sympification attempts will be aborted very early on.

    """

    __slots__ = ['_func']

    def __init__(self, func):
        """Initialize the object."""
        self._func = func

    def __call__(self, *args, **kwargs):
        """Dispatch to the wrapped callable."""
        return self._func(*args, **kwargs)


class _EnumSymbsMeta(type):
    """The meta class for enumeration symbols.

    The primary purpose of this metaclass is to set the concrete singleton
    values from the enumerated symbols set in the class body.
    """

    SYMBS_INPUT = '_symbs_'

    def __new__(mcs, name, bases, attrs):
        """Create the new concrete symbols class."""

        cls = super().__new__(mcs, name, bases, attrs)

        if not hasattr(cls, mcs.SYMBS_INPUT):
            raise AttributeError('Cannot find attribute ' + mcs.SYMBS_INPUT)

        symbs = getattr(cls, mcs.SYMBS_INPUT)
        if symbs is None:
            # Base class.
            return cls

        if not isinstance(symbs, Sequence):
            raise ValueError('Invalid symbols', symbs, 'expecting a sequence')
        for i in symbs:
            invalid = not isinstance(i, Sequence) or len(i) != 2 or any(
                not isinstance(j, str) for j in i
            )
            if invalid:
                raise ValueError(
                    'Invalid symbol', i,
                    'expecting pairs of identifier and LaTeX form.'
                )
        if len(symbs) < 2:
            raise ValueError(
                'Invalid symbols ', symbs, 'expecting multiple of them'
            )

        for i, v in enumerate(symbs):
            obj = cls(i)
            setattr(cls, v[0], obj)
            continue

        return cls


class EnumSymbs(AtomicExpr, metaclass=_EnumSymbsMeta):
    """Base class for enumeration symbols.

    Subclasses can set `_symbs_` inside the class body to be a sequence of
    string pairs.  Then attributes named after the first field of the pairs will
    be created, with the LaTeX form controlled by the second field of the pair.

    The resulted values are valid SymPy expressions.  They are ordered according
    to their order in the given enumeration sequence.

    """

    _symbs_ = None

    _VAL_FIELD = '_val_index'
    __slots__ = [_VAL_FIELD]

    def __init__(self, val_index):
        """Initialize the concrete symbol object.
        """
        if self._symbs_ is None:
            raise ValueError('Base EnumSymbs class cannot be instantiated')
        setattr(self, self._VAL_FIELD, val_index)

    @property
    def args(self):
        """The argument for SymPy."""
        return Integer(getattr(self, self._VAL_FIELD)),

    def __str__(self):
        """Get the string representation of the symbol."""
        return self._symbs_[getattr(self, self._VAL_FIELD)][0]

    def __repr__(self):
        """Get the machine readable string representation."""
        return '.'.join([type(self).__name__, str(self)])

    _op_priority = 20.0

    def __eq__(self, other):
        """Test two values for equality."""
        return isinstance(other, type(self)) and self.args == other.args

    def __hash__(self):
        """Hash the concrete symbol object."""
        return hash(repr(self))

    def __lt__(self, other):
        """Test two values for less than order.

        The order will be based on the order given in the class.
        """
        return self.args < other.args

    def __gt__(self, other):
        """Test two values for greater than."""
        return self.args > other.args

    def __sub__(self, other: Expr):
        """Subtract the current value with another.

        This method is mainly to be able to work together with the Kronecker
        delta class from SymPy.  The difference is only guaranteed to have
        correct ``is_zero`` property.  The actual difference might not make
        mathematical sense.
        """

        if isinstance(other, type(self)):
            return self.args[0] - other.args[0]
        elif len(other.atoms(Symbol)) == 0:
            raise ValueError(
                'Invalid operation for ', (self, other),
                'concrete symbols can only be subtracted for the same type'
            )
        else:
            # We are having a symbolic value at the other expression.  We just
            # need to make sure that the result is fuzzy.
            assert other.is_zero is None
            return other

    def __rsub__(self, other):
        """Subtract the current value from the other expression.

        Only the ``is_zero`` property is guaranteed.
        """
        return self.__sub__(other)

    def __getstate__(self):
        """Retrieve state of object for pickling/serialization"""
        return {slot: getattr(self, slot) for slot in self.__slots__}

    def __setstate__(self, state):
        """Create state of object for deserialization"""
        for key, value in state.items():
            setattr(self, key, value)

    def sort_key(self, order=None):
        return (
            self.class_key(),
            (1, tuple(i.sort_key() for i in self.args)),
            S.One.sort_key(), S.One
        )

    def _latex(self, _):
        """Print itself as LaTeX code."""
        return self._symbs_[self.args[0]][1]


#
# Spark utilities
# ---------------
#


class BCastVar:
    """Automatically broadcast variables.

    This class is a shallow encapsulation of a variable and its broadcast
    into the spark context.  The variable can be redistributed automatically
    after any change.

    """

    __slots__ = [
        '_var'
    ]

    def __init__(self, var):
        """Initialize the variable storage."""
        self._var = var

    @property
    def var(self):
        """Get the variable to mutate."""
        return self._var

    @property
    def ro(self):
        """Get the variable, read-only.

        Note that this function only prevents the redistribution of the
        variable.  It cannot force the variable not be mutated.
        """
        return self._var

    @property
    def bcast(self):
        """Get the variable (no broadcasting in simplified version)."""
        return self


def nest_bind(bag: db.Bag, func, full_balance=True):
    """Nest the flat map of the given function.

    When an entry no longer need processing, None can be returned by the call
    back function.

    """

    if full_balance:
        return _nest_bind_full_balance(bag, func)
    else:
        return _nest_bind_no_balance(bag, func)


def _nest_bind_full_balance(bag: db.Bag, func):
    """Nest the flat map of the given function with full load balancing.
    """

    def wrapped(obj):
        """Wrapped function for nest bind."""
        vals = func(obj)
        if vals is None:
            return [(False, obj)]
        else:
            return [(True, i) for i in vals]

    curr = bag
    res = []
    while curr.count().compute() > 0:
        step_res = curr.map(wrapped).flatten()
        new_entries = step_res.filter(lambda x: not x[0]).map(lambda x: x[1])
        res.append(new_entries)
        curr = step_res.filter(lambda x: x[0]).map(lambda x: x[1])

    # Union all results
    if res:
        return db.concat(res)
    else:
        return db.from_sequence([])


def _nest_bind_no_balance(bag: db.Bag, func):
    """Nest the flat map of the given function without load balancing.
    """

    def wrapped(obj):
        """Wrapped function for nest bind."""
        curr = [obj]
        res = []
        while len(curr) > 0:
            new_curr = []
            for i in curr:
                step_res = func(i)
                if step_res is None:
                    res.append(i)
                else:
                    new_curr.extend(step_res)
                continue
            curr = new_curr
            continue

        return res

    return bag.map(wrapped).flatten()


#
# Misc utilities
# --------------
#

def ensure_pair(obj, role):
    """Ensures that the given object is a pair."""
    if not (isinstance(obj, Sequence) and len(obj) == 2):
        raise TypeError('Invalid {}: '.format(role), obj, 'expecting pair')
    return obj


_ALNUM = frozenset(
    j
    for i in [string.ascii_letters, string.digits]
    for j in i
)


def extract_alnum(inp: str):
    """Extract the alpha numeric part of the string.

    This function is mostly for generating valid identifiers for objects with a
    mathematically formatted name.
    """
    return ''.join(i for i in inp if i in _ALNUM)


#
# Small user utilities
# --------------------
#

def sum_(obj):
    """Sum the values in the given iterable.

    Different from the built-in summation function, the summation is based on
    the first item in the iterable.   Or a SymPy integer zero is created
    when the iterator is empty.
    """

    i = iter(obj)
    try:
        init = next(i)
    except StopIteration:
        return Integer(0)
    else:
        return functools.reduce(operator.add, i, init)


def prod_(obj):
    """Product the values in the given iterable.

    Similar to the summation utility function :py:func:`sum_`, here the initial
    value for the reduction is the first element.  Different from the summation,
    here a SymPy integer unity will be returned for empty iterator.
    """

    i = iter(obj)
    try:
        init = next(i)
    except StopIteration:
        return Integer(1)
    else:
        return functools.reduce(operator.mul, i, init)


class Stopwatch:
    """Utility class for printing timing information.

    This class helps to timing the progression of batch jobs.  It is capable of
    getting and formatting the elapsed wall time between consecutive steps.
    Note that the timing here might not be accurate to one second.

    """

    def __init__(self, print_cb=print):
        """Initialize the stopwatch.

        Parameters
        ----------

        print_cb
            The function will be called with the formatted time-stamp.  By
            default, it will just be written to stdout.

        """
        self._print = print_cb
        self.tick(total=True)

    def tick(self, total=False):
        """Reset the timer.

        Parameters
        ----------

        total
            If the total beginning time is going to be reset as well.

        """
        self._prev = time.time()
        if total:
            self._begin = self._prev

    def tock(self, label, tensor=None):
        """Make a timestamp.

        The formatted timestamp will be given to the callback of the current
        stamper.  The wall time elapsed since the last :py:meth:`tick` will be
        printed.

        Parameters
        ----------

        label
            The label for the current step.

        tensor
            When a tensor is given, it will be cached, counted its number of
            terms.  This method has this parameter since if no reduction is
            performed on the tensor, it might remain unevaluated inside Spark
            and give misleading timing information.

        """

        if tensor is not None:
            tensor.cache()
            n_terms = '{} terms, '.format(tensor.n_terms)
        else:
            n_terms = ''

        now = time.time()
        elapse = now - self._prev
        self._prev = now

        self._print(
            '{} done, {}wall time: {:.2f} s'.format(label, n_terms, elapse)
        )

    def tock_total(self):
        """Make a timestamp for the total time.

        The total time will be the time elapsed since the **total** time was
        last reset.
        """

        now = time.time()
        self._print(
            'Total wall time: {:.2f} s'.format(now - self._begin)
        )


class CallByIndex:
    """Wrapper over callables such that they can be called by indexing.

    This wrapper can be helpful for cases where an indexable object is expected
    but flexibility of a callable is needed.  The given object will be wrapped
    inside and called when the wrapper is indexed.

    """

    __slots__ = ['_callable']

    def __init__(self, callable):
        """Initialize the object."""
        self._callable = callable

    def __getitem__(self, item):
        """Get the item by calling the given callable."""
        return self._callable(item)


class InvariantIndexable(CallByIndex):
    """Objects whose indexing always gives the same constant.

    This small utility is for cases where we need an indexable object whose
    indexing result is actually invariant with respect to the given indices.
    For an instance constructed with value ``v``, all indexing of it gives ``v``
    back.

    """

    __slots__ = []

    def __init__(self, v):
        """Initialize the invariant tensor."""
        super().__init__(lambda _: v)


class SymbResolver:
    """Resolver based on symbols.

    It can be given an iterable of range/symbols pairs telling that the symbols
    are associated with the key ranges.  In strict mode, only the given symbols
    can be resolved to be in the given range.  In non-strict mode, all
    expressions having one of the symbols will be resolved to be in the range of
    the symbol.

    Behaviour is undefined if we have non-disjoint symbol sets for different
    ranges or when we have expression containing symbols for multiple known
    ranges.

    """

    __slots__ = [
        '_known',
        '_strict'
    ]

    def __init__(self, range_symbs, strict):
        """Initialize the resolver."""

        known = {}
        self._known = known
        for range_, dumms in range_symbs:
            for i in dumms:
                known[i] = range_
                continue
            continue

        self._strict = strict

    def __call__(self, expr: Expr):
        """Try to resolve an expression."""

        known = self._known
        if self._strict:
            if expr in known:
                return known[expr]
        else:
            for i in expr.atoms(Symbol):
                if i in known:
                    return known[i]
                continue

        return None
