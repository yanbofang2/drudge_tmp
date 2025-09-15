"""Tests for the basic tensor facilities using free algebra."""

import io
import os
import os.path
import pickle
import shutil

import pytest
from sympy import (
    sympify, IndexedBase, sin, cos, KroneckerDelta, symbols, conjugate, Wild,
    Rational, Symbol, Function
)

from drudge import (
    Drudge, Range, Vec, Term, Perm, NEG, CONJ, TensorDef, CR, UP, DOWN
)

from conftest import skip_in_distributed


@pytest.fixture
def free_alg(local_ctx):
    """Initialize the environment for a free algebra."""

    dr = Drudge(local_ctx)

    r = Range('R')
    dumms = sympify('i, j, k, l, m, n')
    dr.set_dumms(r, dumms)

    s = Range('S')
    s_dumms = symbols('alpha beta')
    dr.set_dumms(s, s_dumms)

    dr.add_resolver_for_dumms()

    # For testing the Einstein over multiple ranges.
    a1, a2 = symbols('a1 a2')
    dr.add_resolver({
        a1: (r, s), a2: (r, s)
    })
    dr.set_name(a1, a2)

    v = Vec('v')
    dr.set_name(v)

    m = IndexedBase('m')
    dr.set_symm(m, Perm([1, 0], NEG))

    h = IndexedBase('h')
    dr.set_symm(h, Perm([1, 0], NEG | CONJ))

    rho = IndexedBase('rho')
    dr.set_symm(rho, Perm([1, 0, 3, 2]), valence=4)

    dr.set_tensor_method('get_one', lambda x: 1)

    return dr


def test_drudge_has_names(free_alg):
    """Test the name archive for drudge objects.

    Here selected names are tested to makes sure all the code are covered.
    """

    p = free_alg.names

    # Range and dummy related.
    assert p.R == Range('R')
    assert len(p.R_dumms) == 6
    assert p.R_dumms[0] == p.i
    assert p.R_dumms[-1] == p.n

    # Vector bases.
    assert p.v == Vec('v')

    # Scalar bases.
    assert p.m == IndexedBase('m')


def test_tensor_can_be_created(free_alg):
    """Test simple tensor creation."""

    dr = free_alg
    p = dr.names
    i, v, r = p.i, p.v, p.R
    x = IndexedBase('x')

    # Create the tensor by two user creation functions.
    for tensor in [
        dr.sum((i, r), x[i] * v[i]),
        dr.einst(x[i] * v[i])
    ]:
        assert tensor.n_terms == 1

        terms = tensor.local_terms
        assert len(terms) == 1
        term = terms[0]
        assert term == Term(((i, r),), x[i], (v[i],))


def test_complex_tensor_creation(free_alg):
    """Test tensor creation involving operations."""

    dr = free_alg
    p = dr.names
    i, v, r = p.i, p.v, p.R
    x = IndexedBase('x')
    for summand in [(x[i] / 2) * v[i], x[i] * (v[i] / 2)]:
        tensor = dr.einst(summand)
        assert tensor.n_terms == 1

        terms = tensor.local_terms
        assert len(terms) == 1
        term = terms[0]
        assert term == Term(((i, r),), x[i] / 2, (v[i],))


def test_tensor_has_basic_operations(free_alg):
    """Test some of the basic operations on tensors.

    Tested in this module:

        1. Addition.
        2. Merge.
        3. Free variable.
        4. Dummy reset.
        5. Equality comparison.
        6. Expansion
        7. Mapping to scalars.
        8. Base presence testing.
    """

    dr = free_alg
    p = dr.names
    i, j, k, l, m = p.R_dumms[:5]
    x = IndexedBase('x')
    r = p.R
    v = p.v
    tensor = (
            dr.sum((l, r), x[i, l] * v[l]) +
            dr.sum((m, r), x[j, m] * v[m])
    )

    # Without dummy resetting, they cannot be merged.
    assert tensor.n_terms == 2
    assert tensor.merge().n_terms == 2

    # Free variables are important for dummy resetting.
    free_vars = tensor.free_vars
    assert free_vars == {x.label, i, j}

    # Reset dummy.
    reset = tensor.reset_dumms()
    expected = (
            dr.sum((k, r), x[i, k] * v[k]) +
            dr.sum((k, r), x[j, k] * v[k])
    )
    assert reset == expected
    assert reset.local_terms == expected.local_terms

    # Merge the terms.
    merged = reset.merge()
    assert merged.n_terms == 1
    term = merged.local_terms[0]
    assert term == Term(((k, r),), x[i, k] + x[j, k], (v[k],))

    # Slightly separate test for expansion.
    c, d = symbols('c d')
    tensor = dr.sum((i, r), x[i] * (c + d) * v[i])
    assert tensor.n_terms == 1
    expanded = tensor.expand()
    assert expanded.n_terms == 2
    # Make sure shallow expansion does not delve into the tree.
    shallowly_expanded = tensor.shallow_expand()
    assert shallowly_expanded.n_terms == 1

    # Make sure shallow expansion does the job on the top-level.
    y = IndexedBase('y')
    tensor = dr.sum((i, r), (x[i] * (c + d) + y[i]) * v[i])
    assert tensor.n_terms == 1
    expanded = tensor.expand()
    assert expanded.n_terms == 3
    shallowly_expanded = tensor.shallow_expand()
    assert shallowly_expanded.n_terms == 2

    # Here we also test concrete summation facility.
    expected = dr.sum(
        (i, r), (j, [c, d]), x[i] * j * v[i]
    )
    assert expected == dr.sum(
        (i, r), x[i] * c * v[i] + x[i] * d * v[i]
    ).expand()

    # Test mapping to scalars.
    tensor = dr.sum((i, r), x[i] * v[i, j])
    y = IndexedBase('y')
    substs = {x: y, j: c}
    res = tensor.map2scalars(lambda x: x.xreplace(substs))
    assert res == dr.sum((i, r), y[i] * v[i, c])
    res = tensor.map2scalars(lambda x: x.xreplace(substs), skip_vecs=True)
    assert res == dr.sum((i, r), y[i] * v[i, j])
    assert res == tensor.map2amps(lambda x: x.xreplace(substs))

    # Test base presence.
    tensor = dr.einst(x[i] * v[i])
    assert tensor.has_base(x)
    assert tensor.has_base(v)
    assert not tensor.has_base(IndexedBase('y'))
    assert not tensor.has_base(Vec('w'))

    # Test Einstein summation over multiple ranges.
    a1, a2 = p.a1, p.a2
    summand = x[a1, a2] * v[a1, a2]
    res = dr.einst(summand).simplify()
    assert res.n_terms == 4
    ranges = (p.R, p.S)
    assert res == dr.sum((a1, ranges), (a2, ranges), summand).simplify()


def test_basic_handling_range_with_variable_bounds(local_ctx):
    """Test the treatment of ranges with variable bounds.

    Here we use a simple example that slightly resembles the angular momentum
    handling in quantum physics.  Here we concentrate on basic operations of
    dummy resetting and mapping of scalar functions.
    """

    dr = Drudge(local_ctx)

    j1, j2 = symbols('j1 j2')
    m1, m2 = symbols('m1, m2')
    j_max = symbols('j_max')
    j = Range('j', 0, j_max)
    m = Range('m')
    dr.set_dumms(j, [j1, j2])
    dr.set_dumms(m, [m1, m2])

    v = Vec('v')
    x = IndexedBase('x')
    tensor = dr.sum((j2, j), (m2, m[0, j2]), x[j2, m2] * v[j2, m2])

    reset = tensor.reset_dumms()
    assert reset.n_terms == 1
    term = reset.local_terms[0]
    assert len(term.sums) == 2
    if term.sums[0][1].label == 'j':
        j_sum, m_sum = term.sums
    else:
        m_sum, j_sum = term.sums
    assert j_sum[0] == j1
    assert j_sum[1].args == j.args
    assert m_sum[0] == m1
    assert m_sum[1].label == 'm'
    assert m_sum[1].lower == 0
    assert m_sum[1].upper == j1  # Important!
    assert term.amp == x[j1, m1]
    assert term.vecs == (v[j1, m1],)

    # Test that functions can be mapped to the bounds.
    repled = reset.map2scalars(
        lambda x: x.xreplace({j_max: 10}), skip_ranges=False
    )
    assert repled.n_terms == 1
    term = repled.local_terms[0]
    checked = False
    for _, i in term.sums:
        if i.label == 'j':
            assert i.lower == 0
            assert i.upper == 10
            checked = True
        continue
    assert checked


def test_handling_of_variable_bound_sums_in_merge(free_alg):
    """A regression test for handling bounds with different variable bounds.
    """

    dr = free_alg
    p = dr.names

    x = IndexedBase('x')
    v = p.v
    i = p.i
    r = p.R

    first = dr.sum((i, r), x[i] * v[i])

    # First trial, when the ranges are really the same.
    assert dr.simplify(
        first - dr.sum((i, r), x[i] * v[i])
    ) == 0

    # This time, they should not be merged.
    assert dr.simplify(
        first - dr.sum((i, r[0, Symbol('n')]), x[i] * v[i])
    ).n_terms == 2


def test_handling_of_variable_bound_sums_in_trivial_summation(free_alg):
    """A regression test for handling bounds with different variable bounds.
    """

    dr = free_alg
    p = dr.names

    v = p.v
    r = p.R
    i = p.i
    alpha = p.alpha
    s = p.S
    n = Symbol('N')

    first = dr.sum((i, r[0, n]), (alpha, s[0, n]), v[alpha])

    # First trial, when the dummy is not actually used.
    assert dr.simplify(
        first
    ) == dr.sum((alpha, s[0, n]), n * v[alpha])

    # When i is used in the bounds of summation over alpha, it should be kept.
    second = dr.sum((i, r[0, n]), (alpha, s[0, i]), v[alpha])
    assert dr.simplify(
        second
    ) == second


def test_adv_merging(free_alg):
    """Test advanced merging options."""

    dr = free_alg
    m, n, a, b, c = symbols('m n a b c')
    orig = m * a * b + n * a * c
    factored = (m * b + n * c) * a
    tensor = dr.sum(orig).expand()
    assert tensor.n_terms == 2

    res = tensor.merge()
    assert res.n_terms == 1
    amp = res.local_terms[0].amp
    assert amp == orig

    res = tensor.merge(consts=(m, n))
    assert res.n_terms == 2

    res = tensor.merge(consts=(m, n, b, c))
    assert res.n_terms == 1
    amp = res.local_terms[0].amp
    assert amp == factored
    assert amp != orig

    res = tensor.merge(gens=(a, b))
    assert res.n_terms == 2

    res = tensor.merge(gens=(a,))
    assert res.n_terms == 1
    amp = res.local_terms[0].amp
    assert amp == factored
    assert amp != orig


def test_tensor_can_be_simplified_amp(free_alg):
    """Test the amplitude simplification for tensors.

    More than trivial tensor amplitude simplification is tested here.  Currently
    it mostly concentrates on the dispatching to SymPy and delta simplification.
    The master simplification is also tested.
    """

    dr = free_alg
    p = dr.names
    r = p.R
    s = p.S
    v = p.v
    i, j = p.R_dumms[:2]
    alpha = p.alpha

    x = IndexedBase('x')
    y = IndexedBase('y')
    theta = sympify('theta')

    tensor = (
            dr.sum((i, r), sin(theta) ** 2 * x[i] * v[i]) +
            dr.sum(
                (i, r), (j, r),
                cos(theta) ** 2 * x[j] * KroneckerDelta(i, j) * v[i]
            ) +
            dr.sum((i, r), (alpha, s), KroneckerDelta(i, alpha) * y[i] * v[i])
    )
    assert tensor.n_terms == 3

    first = tensor.simplify_deltas().simplify_amps()
    # Now we should have one term killed.
    assert first.n_terms == 2

    # Merge again should really simplify.
    merged = first.reset_dumms().merge().simplify_amps()
    assert merged.n_terms == 1
    expected = dr.sum((i, r), x[i] * v[i])
    assert merged == expected

    # The master simplification should do it in one turn.
    simpl = tensor.simplify()
    assert simpl == expected


def test_simplify_delta_of_two_ranges(free_alg):
    """Test simplification of delta of two disjoint ranges."""

    dr = free_alg
    p = dr.names
    tensor = dr.sum(KroneckerDelta(p.i, p.alpha) * p.v)
    assert tensor.n_terms == 1
    assert tensor.simplify_deltas() == 0
    assert tensor.simplify() == 0


@skip_in_distributed(reason="SymPy Function serialization issues")
def test_simplify_delta_of_unsolvable_functions(free_alg):
    """Test simplification of delta of with functions unable to solve."""

    dr = free_alg
    p = dr.names
    f = Function('f')
    tensor = dr.sum(
        (p.i, p.R), KroneckerDelta(f(p.i), p.alpha) * p.v
    )
    assert tensor.n_terms == 1
    assert tensor.simplify_deltas() == tensor
    assert tensor.simplify() == tensor


def test_tensor_can_be_canonicalized(free_alg):
    """Test tensor canonicalization in simplification.

    The master simplification function is tested, the core simplification is at
    the canonicalization.  Equality testing with zero is also tested.
    """

    dr = free_alg
    p = dr.names
    i, j = p.R_dumms[:2]
    r = p.R
    m = p.m
    h = p.h
    v = p.v

    # Anti-symmetric real matrix.
    tensor = (
            dr.sum((i, r), (j, r), m[i, j] * v[i] * v[j]) +
            dr.sum((i, r), (j, r), m[j, i] * v[i] * v[j])
    )
    assert tensor.n_terms == 2
    res = tensor.simplify()
    assert res == 0

    # With wrapping under an even function.
    tensor = (
            dr.sum((i, r), (j, r), m[i, j] ** 2 * v[i] * v[j]) +
            dr.sum((i, r), (j, r), m[j, i] ** 2 * v[i] * v[j])
    )
    assert tensor.n_terms == 2
    res = tensor.simplify()
    assert res.n_terms == 1
    term = res.local_terms[0]
    assert term.sums == ((i, r), (j, r))
    assert term.amp == 2 * m[i, j] ** 2
    assert term.vecs == (v[i], v[j])

    # With wrapping under an odd function.
    tensor = (
            dr.sum((i, r), (j, r), m[i, j] ** 3 * v[i] * v[j]) +
            dr.sum((i, r), (j, r), m[j, i] ** 3 * v[i] * v[j])
    )
    assert tensor.n_terms == 2
    res = tensor.simplify()
    assert res.n_terms == 0

    # Hermitian matrix.
    tensor = dr.einst(
        h[i, j] * v[i] * v[j] + conjugate(h[j, i]) * v[i] * v[j]
    )
    assert tensor.n_terms == 2
    res = tensor.simplify()
    assert res == 0


class SymmFunc(Function):
    """A symmetric function."""
    pass


def test_tensors_w_functions_can_be_canonicalized(free_alg):
    """Test canonicalization facility on general functions."""
    dr = free_alg
    dr.set_symm(SymmFunc, Perm([1, 0], NEG), valence=2, set_base_name=False)

    p = dr.names
    i, j, k = p.R_dumms[:3]
    r = p.R
    v = p.v

    # General anti-symmetric real matrix.
    tensor = dr.sum(
        (i, r), (j, r), SymmFunc(k, i, j) * SymmFunc(i, j) * v[i] * v[j]
    ) + dr.sum(
        (i, r), (j, r), SymmFunc(k, i, j) * SymmFunc(j, i) * v[i] * v[j]
    )
    assert tensor.n_terms == 2
    assert tensor.simplify() == 0


def test_canonicalization_of_vectors_w_symm(free_alg):
    """Test the canonicalization when vectors are given (anti-)symmetries.
    """

    dr = free_alg
    p = dr.names
    x = IndexedBase('x')
    r = p.R
    i, j = p.i, p.j

    vs = Vec('vs')
    dr.set_symm(vs, Perm([1, 0]), valence=2)
    tensor = dr.sum((i, r), (j, r), x[i, j] * vs[j, i])
    res = tensor.simplify()
    assert res.n_terms == 1
    term = res.local_terms[0]
    assert term.sums == ((i, r), (j, r))
    assert term.amp == x[i, j]
    assert term.vecs == (vs[i, j],)

    va = Vec('va')
    dr.set_symm(va, Perm([1, 0], NEG), valence=2)
    tensor = dr.sum((i, r), (j, r), x[i, j] * va[j, i])
    res = tensor.simplify()
    assert res.n_terms == 1
    term = res.local_terms[0]
    assert term.sums == ((i, r), (j, r))
    assert term.amp == -x[i, j]
    assert term.vecs == (va[i, j],)


def test_canonicalization_connected_summations(free_alg):
    """Test canonicalization where the summations has relationships."""
    dr = free_alg
    p = dr.names
    i, j, k, l = p.R_dumms[:4]
    a, b = symbols('a b')
    t = IndexedBase('t')

    tensor = dr.sum(
        (i, p.R), (j, p.R[0, i]), (k, p.R[0, a]), (l, p.R[0, b]),
        t[i] * t[j] * t[k] * t[l]
    )
    res = tensor.simplify()
    sums = res.local_terms[0].sums

    # The desired order for the summations.
    assert not sums[0][1].bounded
    assert sums[1][1].upper == a
    assert sums[2][1].upper == b
    assert sums[3][1].upper == sums[0][0]


def test_tensor_math_ops(free_alg):
    """Test tensor math operations.

    Mainly here we test addition, multiplication, and division.
    """

    dr = free_alg
    p = dr.names
    r = p.R
    v = p.v
    w = Vec('w')
    x = IndexedBase('x')
    i, j, k = p.R_dumms[:3]
    a = sympify('a')

    v1 = dr.sum((i, r), x[i] * v[i])
    w1 = dr.sum((i, r), x[i] * w[i])
    assert v1.n_terms == 1
    assert w1.n_terms == 1

    v1_neg = -v1
    assert v1_neg == dr.sum((i, r), -x[i] * v[i])

    v1_1 = v1 + 2
    assert v1_1.n_terms == 2
    assert v1_1 == 2 + v1

    w1_1 = w1 + a
    assert w1_1.n_terms == 2
    assert w1_1 == a + w1

    prod = v1_1 * w1_1
    # Test scalar multiplication here as well.
    expected = (
            2 * a + a * v1 + 2 * w1 +
            dr.sum((i, r), (j, r), x[i] * x[j] * v[i] * w[j])
    )
    assert prod.simplify() == expected.simplify()

    # Test the commutator operation.
    comm_v1v1 = v1 | v1
    assert comm_v1v1.simplify() == 0
    # Here the tensor subtraction can also be tested.
    comm_v1w1 = v1 | w1
    expected = (
            dr.sum((i, r), (j, r), x[i] * x[j] * v[i] * w[j]) -
            dr.sum((i, r), (j, r), x[j] * x[i] * w[i] * v[j])
    )
    assert comm_v1w1.simplify() == expected.simplify()

    alpha = symbols('alpha')
    assert alpha not in v1.free_vars
    tensor = v1 / alpha
    assert tensor.n_terms == 1
    terms = tensor.local_terms
    assert len(terms) == 1
    term = terms[0]
    assert term.sums == ((i, r),)
    assert term.amp == x[i] / alpha
    assert term.vecs == (v[i],)
    assert alpha in tensor.free_vars


def test_trivial_sums_can_be_simplified(free_alg):
    """Test the simplification facility for trivial sums."""
    dr = free_alg
    r = Range('D', 0, 2)

    a, b = symbols('a b')
    tensor = dr.sum(1) + dr.sum((a, r), 1) + dr.sum((a, r), (b, r), 1)
    res = tensor.simplify()
    assert res == dr.sum(7)


def test_amp_sums_can_be_simplified(free_alg):
    """Test the simplification facility for more complex amplitude sums."""
    dr = free_alg
    v = dr.names.v
    n, i, j = symbols('n i j')
    x = IndexedBase('x')
    r = Range('D', 0, n)

    tensor = dr.sum((i, r), (j, r), i ** 2 * x[j] * v[j])
    res = tensor.simplify_sums()
    assert res == dr.sum((j, r), (
            n ** 3 / 3 - n ** 2 / 2 + n / 6
    ) * x[j] * v[j])


def test_tensors_can_be_differentiated(free_alg):
    """Test the analytic gradient computation of tensors."""

    dr = free_alg
    p = dr.names

    a = IndexedBase('a')
    b = IndexedBase('b')
    i, j, k, l, m, n = p.R_dumms[:6]

    tensor = dr.einst(
        a[i, j, k, l] * b[i, j] * conjugate(b[k, l])
    )

    # Test real analytic gradient.

    res = tensor.diff(b[i, j], real=True)
    expected = dr.einst(
        b[k, l] * (a[k, l, i, j] + a[i, j, k, l])
    )
    assert (res - expected).simplify() == 0

    # Test Wirtinger complex derivative.
    res, res_conj = [
        tensor.diff(b[m, n], wirtinger_conj=conj)
        for conj in [False, True]
    ]

    expected = dr.einst(
        conjugate(b[i, j]) * a[m, n, i, j]
    )
    expect_conj = dr.einst(
        a[i, j, m, n] * b[i, j]
    )

    for res_i, expected_i in [(res, expected), (res_conj, expect_conj)]:
        assert (res_i - expected_i).simplify() == 0

    # Test real analytic gradient with a simple test case.

    tensor = dr.einst(b[i, j] * b[j, i])
    grad = tensor.diff(b[i, j])
    assert (grad - 2 * b[j, i]).simplify() == 0


@pytest.mark.parametrize('full_balance', [True, False])
def test_tensors_can_substitute_scalars(free_alg, full_balance):
    """Test scalar substitution facility for tensors."""

    dr = free_alg
    p = dr.names

    x = IndexedBase('x')
    y = IndexedBase('y')
    z = IndexedBase('z')
    r = p.R
    i, j, k, l, m = p.R_dumms[:5]

    x_def = dr.define(
        x[i], dr.sum((j, r), y[j] * z[i])
    )
    orig = dr.sum((i, r), x[i] ** 2 * x[k])

    # k is free.
    expected = dr.sum(
        (i, r), (j, r), (l, r), (m, r),
        z[i] ** 2 * y[j] * y[l] * y[m] * z[k]
    )

    # Test different ways to perform the substitution.
    for res in [
        orig.subst(x[i], x_def.rhs, full_balance=full_balance),
        orig.subst_all([x_def], full_balance=full_balance),
        orig.subst_all([(x[i], x_def.rhs)], full_balance=full_balance),
        x_def.act(orig, full_balance=full_balance)
    ]:
        assert res.simplify() == expected.simplify()


@pytest.mark.parametrize('full_balance', [True, False])
@pytest.mark.parametrize('full_simplify', [True, False])
def test_tensors_can_substitute_vectors(
        free_alg, full_balance, full_simplify
):
    """Test vector substitution facility for tensors."""

    dr = free_alg
    p = dr.names

    x = IndexedBase('x')
    t = IndexedBase('t')
    u = IndexedBase('u')
    i, j = p.i, p.j
    v = p.v
    w = Vec('w')

    orig = dr.einst(x[i] * v[i])
    v_def = dr.einst(t[i, j] * w[j] + u[i, j] * w[j])

    dr.full_simplify = full_simplify
    res = orig.subst(v[i], v_def, full_balance=full_balance).simplify()
    dr.full_simplify = True

    expected = dr.einst(
        x[i] * t[i, j] * w[j] + x[i] * u[i, j] * w[j]
    ).simplify()
    assert res == expected


@pytest.mark.parametrize('full_balance', [True, False])
def test_numbers_can_substitute_scalars(free_alg, full_balance):
    """Test substituting scalars with numbers."""

    dr = free_alg
    p = dr.names

    x = IndexedBase('x')
    y = IndexedBase('y')
    r = Range('D', 0, 2)
    i, j, k, l = symbols('i j k l')
    dr.set_dumms(r, [i, j, k, l])
    v = p.v

    orig = dr.sum((i, r), x[i] ** 2 * x[j] * y[k] * v[l])

    res = orig.subst(x[i], 0, full_balance=full_balance).simplify()
    assert res == 0
    res = orig.subst(x[j], 1, full_balance=full_balance).simplify()
    assert res == dr.sum(2 * y[k] * v[l])
    res = orig.subst(x[k], 2, full_balance=full_balance).simplify()
    assert res == dr.sum(16 * y[k] * v[l])


@pytest.mark.parametrize('full_balance', [True, False])
def test_numbers_can_substitute_vectors(free_alg, full_balance):
    """Test substituting vectors with numbers."""

    dr = free_alg
    p = dr.names

    x = IndexedBase('x')
    y = IndexedBase('y')
    r = p.R
    i, j, k, l = symbols('i j k l')
    v = p.v
    w = Vec('w')

    orig = dr.sum((i, r), (j, r), x[i, j] * v[i] * w[j] + y[i, j] * v[i] * v[j])

    res = orig.subst(v[k], 0, full_balance=full_balance).simplify()
    assert res == 0
    res = orig.subst(v[i], 1, full_balance=full_balance).simplify()
    assert res == dr.sum((i, r), (j, r), x[j, i] * w[i] + y[i, j])


@pytest.mark.parametrize('full_balance', [True, False])
def test_tensors_can_substitute_scalars_simultaneously(
        free_alg, full_balance
):
    """Test scalar substitution facility for tensors."""

    dr = free_alg
    p = dr.names

    x = IndexedBase('x')
    r = p.R
    i, j = p.R_dumms[:2]

    x_def = dr.define(x[i], dr.sum(2 * x[i]))
    summand = x[i] ** 2 * x[j]
    orig = dr.sum((i, r), summand)

    # k is free.
    expected = dr.sum((i, r), summand * 2 ** 3)

    # Test different ways to perform the substitution.
    for res in [
        orig.subst(x[i], x_def.rhs, full_balance=full_balance),
        orig.subst_all([x_def], full_balance=full_balance),
        orig.subst_all([(x[i], x_def.rhs)], full_balance=full_balance),
        x_def.act(orig, full_balance=full_balance)
    ]:
        assert res.simplify() == expected.simplify()


@pytest.mark.parametrize('full_balance', [True, False])
@pytest.mark.parametrize('full_simplify', [True, False])
def test_tensors_can_substitute_vectors_simultaneously(
        free_alg, full_balance, full_simplify
):
    """Test vector substitution facility for tensors."""

    dr = free_alg
    p = dr.names

    x = IndexedBase('x')
    i, j = p.i, p.j
    v = p.v

    orig = dr.einst(x[i, j] * v[i] * v[j])
    v_def = dr.sum(2 * v[i])

    dr.full_simplify = full_simplify
    res = orig.subst(v[i], v_def, full_balance=full_balance).simplify()
    dr.full_simplify = True

    expected = dr.einst(4 * x[i, j] * v[i] * v[j]).simplify()
    assert res == expected


@pytest.mark.parametrize('full_balance', [True, False])
@pytest.mark.parametrize('full_simplify', [True, False])
def test_tensors_can_substitute_symbols_simultaneously(
        free_alg, full_balance, full_simplify
):
    """Test vector substitution facility for tensors."""

    dr = free_alg
    p = dr.names

    x = IndexedBase('x')
    alpha = Symbol('alpha')
    beta = IndexedBase('beta')
    i, j, k = p.i, p.j, p.k
    v = p.v

    orig = dr.einst(alpha ** 2 * x[i] * v[i])
    alpha_def = dr.einst(alpha * beta[i, i])
    assert alpha_def.n_terms == 1
    assert len(alpha_def.local_terms[0].sums) == 1

    dr.full_simplify = full_simplify
    res = orig.subst(alpha, alpha_def, full_balance=full_balance).simplify()
    dr.full_simplify = True

    expected = dr.einst(
        alpha ** 2 * beta[i, i] * beta[j, j] * x[k] * v[k]
    ).simplify()
    assert res == expected


@pytest.mark.parametrize('full_balance', [True, False])
@pytest.mark.parametrize('full_simplify', [True, False])
def test_tensors_can_substitute_strings_of_vectors(
        free_alg, full_balance, full_simplify
):
    """Test vector substitution facility for strings of tensors."""

    dr = free_alg
    p = dr.names

    x = IndexedBase('x')
    t = IndexedBase('t')
    u = IndexedBase('u')
    i, j = p.i, p.j
    v = p.v
    w = Vec('w')

    orig = dr.sum((i, p.R), x[i] * v[i] * v[i])
    vivi_def = dr.einst(t[i, j] * w[j] + u[i, j] * w[j])

    dr.full_simplify = full_simplify
    res = orig.subst(
        v[i] * v[i], vivi_def, full_balance=full_balance
    ).simplify()
    dr.full_simplify = True

    expected = dr.einst(
        x[i] * t[i, j] * w[j] + x[i] * u[i, j] * w[j]
    ).simplify()
    assert res == expected

    # Check that it does not wrongly substitute vectors that cannot match the
    # pattern.
    orig = dr.einst(x[i, j] * v[i] * v[j])
    res = orig.subst(v[i] * v[i], vivi_def)
    assert res == orig


@pytest.mark.parametrize('full_balance', [True, False])
@pytest.mark.parametrize('simplify', [True, False])
def test_batch_vector_substitutions(
        free_alg, full_balance, simplify
):
    """Test the batch substitutions using the subst_all method
    """

    dr = free_alg
    p = dr.names

    a = IndexedBase('a')
    x = IndexedBase('x')
    y = IndexedBase('y')
    i, j = p.i, p.j
    v = p.v
    v_dag = Vec('v', indices=(CR,))

    #
    # Spin flipping
    #

    orig1 = dr.sum((i, p.R), (j, p.R), a[i, j] * v[i, UP] * v[j, DOWN])
    defs1 = [
        dr.define(v[i, UP], v[i, DOWN]), dr.define(v[i, DOWN], v[i, UP])
    ]

    # Sequentially apply the definitions of the substitutions
    expected_sequential = dr.sum(
        (i, p.R), (j, p.R), a[i, j] * v[i, UP] * v[j, UP]
    )
    res = orig1.subst_all(
        defs1, simult_all=False, full_balance=full_balance, simplify=simplify
    )
    assert res == expected_sequential

    # Simultaneously apply the definitions of the substitutions
    expected_simutaneous = dr.sum(
        (i, p.R), (j, p.R), a[i, j] * v[i, DOWN] * v[j, UP]
    )
    res = orig1.subst_all(
        defs1, simult_all=True, full_balance=full_balance, simplify=simplify
    )
    assert res == expected_simutaneous

    #
    # In-place BCS transformation
    #

    orig2 = dr.einst(
        a[i, j] * v_dag[i, UP] * v[j, UP] +
        a[i, j] * v_dag[i, DOWN] * v[j, DOWN]
    )
    defs2 = [
        dr.define(v_dag[i, UP], x[i] * v_dag[i, UP] - y[i] * v[i, DOWN]),
        dr.define(v_dag[i, DOWN], x[i] * v_dag[i, DOWN] + y[i] * v[i, UP]),
        dr.define(v[i, UP], x[i] * v[i, UP] - y[i] * v_dag[i, DOWN]),
        dr.define(v[i, DOWN], x[i] * v[i, DOWN] + y[i] * v_dag[i, UP]),
    ]

    # Sequentially apply the definitions of the substitutions
    expected_sequential = orig2
    for def_ in defs2:
        expected_sequential = def_.act(expected_sequential)
    expected_sequential = expected_sequential.simplify()
    res = orig2.subst_all(
        defs2, simult_all=False, full_balance=full_balance, simplify=simplify
    ).simplify()
    assert res == expected_sequential

    # Simultaneously apply the definitions of the substitutions
    expected_simutaneous = dr.sum(
        (i, p.R), (j, p.R), a[i, j] * (
            (x[i] * v_dag[i, UP] - y[i] * v[i, DOWN])
            * (x[j] * v[j, UP] - y[j] * v_dag[j, DOWN])
            + (x[i] * v_dag[i, DOWN] + y[i] * v[i, UP])
            * (x[j] * v[j, DOWN] + y[j] * v_dag[j, UP])
        )
    ).simplify()
    res = orig2.subst_all(
        defs2, simult_all=True, full_balance=full_balance, simplify=simplify
    ).simplify()
    assert res == expected_simutaneous


@pytest.mark.parametrize('full_balance', [True, False])
def test_batch_amp_substitutions(free_alg, full_balance):
    """Test the batch amplitude substitutions using the subst_all method
    """

    dr = free_alg
    p = dr.names

    a = IndexedBase('a')
    b = Symbol('b')
    i = p.i
    r = p.R
    v = p.v

    orig = dr.einst(b * a[i] * v[i])
    defs = [
        dr.define(a[i], a[i] + b),
        dr.define(b, sin(b))
    ]

    # Sequentially apply the definitions of the substitutions
    expected_sequential = dr.sum(
        (i, r), sin(b) * (a[i] + sin(b)) * v[i]
    ).simplify()
    res = orig.subst_all(
        defs, simult_all=False, full_balance=full_balance, simplify=True
    )
    assert res == expected_sequential

    # Simultaneously apply the definitions of the substitutions
    expected_simutaneous = dr.sum(
        (i, r), sin(b) * (a[i] + b) * v[i]
    ).simplify()
    res = orig.subst_all(
        defs, simult_all=True, full_balance=full_balance, simplify=True
    )
    assert res == expected_simutaneous


def test_special_substitution_of_identity(free_alg):
    """Test the special substitution of integer one standing for identity.
    """

    dr = free_alg
    p = dr.names

    x = IndexedBase('x')
    t = IndexedBase('y')
    a = IndexedBase('a')
    i, j = p.i, p.j
    v = p.v
    w = Vec('w')

    orig = dr.sum((i, p.R), x[i] * v[i] + a[i])
    ident_def = dr.define(1, dr.einst(t[i] * w[i]))

    res = orig.subst_all([ident_def])
    assert dr.simplify(
        res - dr.einst(x[i] * v[i])
        - dr.sum((i, p.R), (j, p.R), a[i] * t[j] * w[j])
    ) == 0


def test_tensors_can_be_rewritten(free_alg):
    """Test the amplitude rewriting facility for given vector patterns."""

    dr = free_alg
    p = dr.names
    v = Vec('v')
    a, b = p.R_dumms[:2]

    x = IndexedBase('x')
    o = IndexedBase('o')
    y = IndexedBase('y')
    z = IndexedBase('z')

    tensor = dr.einst(
        x[a] * v[a] + o[a, b] * y[b] * v[a] + z[b] * v[b]  # Terms to rewrite.
        + z[a, b] * v[a] * v[b]  # Terms to keep.
    )

    w = Wild('w')
    r = IndexedBase('r')
    rewritten, defs = tensor.rewrite(v[w], r[w])

    assert rewritten == dr.einst(
        z[a, b] * v[a] * v[b] + r[a] * v[a] + r[b] * v[b]
    )
    assert len(defs) == 2
    assert r[a] in defs
    assert defs[r[a]] == dr.einst(x[a] + o[a, b] * y[b])
    assert r[b] in defs
    assert defs[r[b]] == dr.sum(z[b])


class x(Function):
    """Get the x-component symbolically."""
    pass


class y(Function):
    """Get the y-component symbolically."""
    pass


def test_sums_can_be_expanded(local_ctx):
    """Test the summation expansion facility.

    Here we have essentially a direct product of two ranges and expand it.  The
    usage here also includes some preliminary steps typical in the usage
    paradigm.
    """

    dr = Drudge(local_ctx)

    comp = Range('P')
    r1, r2 = symbols('r1, r2')
    dr.set_dumms(comp, [r1, r2])

    a = IndexedBase('a')
    v = Vec('v')

    # A simple thing written in terms of composite indices.
    orig = dr.sum((r1, comp), (r2, comp), a[r1] * a[r2] * v[r1] * v[r2])

    # Rewrite the expression in terms of components.  Here, r1 should be
    # construed as a simple Wild.
    rewritten = orig.subst_all([
        (a[r1], a[x(r1), y(r1)]),
        (v[r1], v[x(r1), y(r1)])
    ])

    # Expand the summation over r.
    x_dim = Range('X')
    y_dim = Range('Y')
    x1, x2 = symbols('x1 x2')
    dr.set_dumms(x_dim, [x1, x2])
    y1, y2 = symbols('y1 y2')
    dr.set_dumms(y_dim, [y1, y2])

    res = rewritten.expand_sums(comp, lambda r: [
        (Symbol(str(r).replace('r', 'x')), x_dim, x(r)),
        (Symbol(str(r).replace('r', 'y')), y_dim, y(r))
    ])

    assert (res - dr.sum(
        (x1, x_dim), (y1, y_dim), (x2, x_dim), (y2, y_dim),
        a[x1, y1] * a[x2, y2] * v[x1, y1] * v[x2, y2]
    )).simplify() == 0


def test_advanced_manipulations(free_alg):
    """Test advanced manipulations of tensors."""
    dr = free_alg
    p = dr.names
    i, j, k = p.i, p.j, p.k

    u = IndexedBase('u')
    v = IndexedBase('v')
    f = Vec('f')

    tensor = dr.einst(u[i, j] * f[j] + v[i, j] * f[j])
    assert tensor.n_terms == 2

    def has_u(term):
        """Test if a term have u tensor."""
        return term.amp.has(u)

    expect = dr.sum((j, p.R), u[i, j] * f[j])
    for res in [
        tensor.filter(has_u),
        tensor.bind(lambda x: [x] if has_u(x) else [])
    ]:
        assert res.n_terms == 1
        assert res == expect

    def subst_i(term):
        """Substitute i index in the terms."""
        return Term(term.sums, term.amp.xreplace({i: k}), term.vecs)

    expect = dr.sum((j, p.R), u[k, j] * f[j] + v[k, j] * f[j])
    for res in [
        tensor.map(subst_i),
        tensor.bind(lambda x: [subst_i(x)]),
        tensor.map2scalars(lambda x: x.xreplace({i: k}))
    ]:
        assert res.n_terms == 2
        assert res == expect

    alpha, beta = symbols('alpha beta')
    assert tensor.bind(
        lambda x: [Term(x.sums, x.amp * i_, x.vecs) for i_ in [alpha, beta]]
    ) == (tensor * alpha + tensor * beta)

    assert tensor.map2scalars(
        lambda x: x.xreplace({j: k})
    ) == dr.sum((j, p.R), u[i, k] * f[k] + v[i, k] * f[k])

    assert tensor.map2scalars(
        lambda x: x.xreplace({j: k}), skip_vecs=True
    ) == dr.sum((j, p.R), u[i, k] * f[j] + v[i, k] * f[j])


def test_tensor_method(free_alg):
    """Test tensor method can be injected."""

    tensor = free_alg.sum(10)
    assert tensor.get_one() == 1

    with pytest.raises(AttributeError):
        tensor.get_two()


def test_creating_empty_tensor_def(free_alg):
    """Test the creation of empty tensor definition."""
    dr = free_alg

    def_ = TensorDef(symbols('a'), (), dr.create_tensor([]))
    assert def_.rhs == 0


def test_tensor_def_creation_and_basic_properties(free_alg):
    """Test basic tensor definition creation and properties.

    Since tensor definitions are more frequently used for scalars, here we
    concentrate more on the scalar quantities than on vectors.
    """

    dr = free_alg
    p = dr.names
    i, j, k = p.R_dumms[:3]

    x = IndexedBase('x')
    o = IndexedBase('o')
    y = IndexedBase('y')

    rhs = o[i, j] * x[j]

    y_def = dr.define(y, (i, p.R), dr.sum((j, p.R), rhs))

    assert y_def.is_scalar
    assert y_def.rhs == dr.einst(rhs)
    assert y_def.lhs == y[i]
    assert y_def.base == y
    assert y_def.exts == [(i, p.R)]

    assert str(y_def) == 'y[i] = sum_{j} o[i, j]*x[j]'
    assert y_def.latex().strip() == r'y_{i} = \sum_{j \in R} x_{j}  o_{i,j}'

    y_def1 = dr.define(y[i], dr.sum((j, p.R), rhs))
    y_def2 = dr.define_einst(y[i], rhs)
    assert y_def1 == y_def
    assert y_def2 == y_def

    # Test the def_ utility.
    assert not dr.default_einst
    dr.default_einst = True
    y_def3 = dr.def_(y[i], rhs)
    dr.default_einst = False
    y_def4 = dr.def_(y[i], rhs)
    assert y_def3 == y_def
    assert y_def4 != y_def
    assert len(y_def4.local_terms) == 1
    assert len(y_def4.local_terms[0].sums) == 0

    # Test name archive utility for tensor definitions.
    dr.set_name(y_def4)
    assert p._y == y
    assert p.y == y_def4
    dr.unset_name(y_def4)
    assert not hasattr(p, '_y')
    assert not hasattr(p, 'y')

    # This tests the `act` method as well.
    assert y_def[1].simplify() == dr.einst(o[1, j] * x[j]).simplify()


def test_einstein_convention(free_alg):
    """Test Einstein summation convention utility.

    In this test, more complex aspects of the Einstein convention facility is
    tested.  Especially for the external indices and definition creation.
    """

    dr = free_alg
    p = dr.names

    o = IndexedBase('o')
    v = IndexedBase('v')
    w = IndexedBase('w')
    i, j, k = p.R_dumms[:3]

    raw_amp_1 = o[i, k] * v[k, j]
    raw_amp_2 = o[i, k] * w[k, j]
    raw_amp = raw_amp_1 + raw_amp_2

    for inp in [raw_amp, dr.sum(raw_amp)]:
        tensor, exts = dr.einst(inp, auto_exts=True)
        assert exts == {i, j}
        terms = tensor.local_terms
        for idx, term in enumerate(terms):
            assert len(term.sums) == 1
            assert term.sums[0] == (k, p.R)
            if idx == 0:
                assert term.amp == raw_amp_1
            elif idx == 1:
                assert term.amp == raw_amp_2
            assert len(term.vecs) == 0
            continue

    # Test the automatic definition formation.
    tensor_def = dr.define_einst('r', raw_amp, auto_exts=True)
    assert len(tensor_def.exts) == 2
    assert tensor_def.exts[0] == (i, p.R)
    assert tensor_def.exts[1] == (j, p.R)
    assert tensor_def.base == IndexedBase('r')
    assert tensor_def.rhs == dr.einst(raw_amp)


def test_tensor_def_simplification(free_alg):
    """Test basic tensor definition simplification and dummy manipulation.
    """

    dr = free_alg
    p = dr.names

    i, j = p.R_dumms[:2]

    x = IndexedBase('x')
    o = IndexedBase('o')
    y = IndexedBase('y')

    y_def = dr.define(
        y, (j, p.R),
        dr.sum((i, p.R), o[j, i] * x[i]) - dr.einst(o[j, i] * x[i])
    )

    reset = y_def.reset_dumms()
    assert reset.base == y_def.base
    assert reset.exts == [(i, p.R)]
    assert reset.lhs == y[i]
    assert reset.rhs == dr.einst(o[i, j] * x[j]) - dr.einst(o[i, j] * x[j])

    simplified = reset.simplify()
    assert simplified.rhs == 0


def test_tensors_has_string_and_latex_form(free_alg, tmpdir):
    """Test the string and LaTeX form representation of tensors."""

    dr = free_alg
    p = dr.names

    v = p.v
    i = p.i
    x = IndexedBase('x')

    tensor = dr.einst(x[i] * v[i] - x[i] * v[i])
    zero = tensor.simplify()

    # The basic string form.
    orig = str(tensor)
    assert orig == 'sum_{i} x[i] * v[i]\n + sum_{i} -x[i] * v[i]'
    assert str(zero) == '0'

    # The LaTeX form.
    expected_terms = [
        r'\sum_{i \in R} x_{i}    \mathbf{v}_{i}',
        r'- \sum_{i \in R} x_{i}    \mathbf{v}_{i}'
    ]
    expected = ' '.join(expected_terms)
    assert tensor.latex() == expected

    assert tensor.latex(sep_lines=True) != expected
    assert tensor.latex(sep_lines=True).replace(r'\\ ', '') == expected

    assert tensor.latex(align_terms=True) != expected
    assert tensor.latex(align_terms=True).replace(' & ', '') == expected

    def proc(form, term, idx):
        """Process the terms in the LaTeX formatting."""
        assert term == tensor.local_terms[idx]
        assert form == expected_terms[idx]
        return 'N'

    assert tensor.latex(proc=proc).replace(' ', '') == 'N + N'.replace(' ', '')

    # Test the reporting facility.
    with tmpdir.as_cwd():
        title = 'Simple report test'
        sect = 'A simple tensor'
        descr = 'Nothing'

        filename = 'freealg.html'
        with dr.report(filename, title) as rep:
            rep.add(sect, tensor, description=descr)

        # Here we just simply test the existence of the file.
        assert os.path.isfile(filename)

        filename = 'freealg.pdf'
        with dr.report(filename, 'Simple report test') as rep:
            rep.add(
                sect, tensor, description=descr, env='dmath', sep_lines=False
            )
            rep.add(
                sect, tensor, description=descr, env='dmath',
                no_sum=True, scalar_mul=r'\invismult'
            )
        assert os.path.isfile('freealg.tex')
        if shutil.which('pdflatex') is not None:
            assert os.path.isfile(filename)

    # Test the printing of pure zero.
    assert zero.latex() == '0'
    assert zero.latex(sep_lines=True) == '0'

    # Test printing of very special tensors with terms being pure plus/minus
    # unity.
    special = dr.sum(1) + dr.sum(-1)
    assert special.n_terms == 2
    res = special.latex()
    assert res.replace(' ', '') == '1-1'

    # Testing printing of vectors without subscripts.
    special = dr.sum(2 * v)
    res = special.latex()
    assert res.replace(' ', '') == r'2  \mathbf{v}'.replace(' ', '')


def test_drudge_has_default_properties(free_alg):
    """Test some basic default properties for drudge objects."""

    assert isinstance(free_alg.num_partitions, int)
    assert free_alg.full_simplify
    assert not free_alg.simple_merge


def test_tensor_can_be_added_summation(free_alg):
    """Test addition of new summations for existing tensors."""

    dr = free_alg
    p = dr.names
    i, j = p.R_dumms[:2]
    x = IndexedBase('x')
    y = IndexedBase('y')

    tensor = dr.sum((i, p.R), x[i, j] * y[j, i])

    for res in [
        dr.einst(tensor),
        dr.sum((j, p.R), tensor)
    ]:
        assert res == dr.einst(x[i, j] * y[j, i])


def test_pickling_tensors(free_alg):
    """Test tensors and definitions can be correctly pickled and unpickled."""

    dr = free_alg
    p = dr.names
    x = IndexedBase('x')
    v = Vec('v')
    b = Vec('b')

    tensor = dr.einst(x[p.i] * v[p.i])
    def_ = dr.define(b, tensor)
    serialized = pickle.dumps([tensor, def_])

    with pytest.raises(ValueError):
        pickle.loads(serialized)

    with dr.pickle_env():
        res = pickle.loads(serialized)

    assert res[0] == tensor
    assert res[1] == def_


def test_memoise(free_alg, tmpdir):
    """Test the memoise facility of drudge."""

    dr = free_alg
    n_calls = [0]
    filename = 'tmp.pickle'
    log = io.StringIO()

    def get_zero():
        n_calls[0] += 1
        return 0

    # Test the reporting facility.
    with tmpdir.as_cwd():
        assert dr.memoize(get_zero, filename, log=log) == 0
        assert dr.memoize(get_zero, filename, log=log) == 0
        assert dr.memoize(get_zero, filename) == 0
        assert n_calls[0] == 1
        assert len(log.getvalue().splitlines()) == 2


TEST_SIMPLE_DRS = """
x[i] <<= 1 / 2 * sum((i, R), m[i] * v[i])
y = sum_(range(10))
n = n_terms(x)
"""


def test_simple_drs(free_alg):
    """Test a simple drudge script."""
    dr = free_alg
    p = dr.names
    env = dr.exec_drs(TEST_SIMPLE_DRS)

    x = Vec('x')
    i = p.i
    def_ = dr.define_einst(x[i], Rational(1, 2) * p.m[i] * p.v[i])
    assert env['x'] == def_
    assert env['_x'] == x
    assert env['y'] == 45
    assert env['n'] == 1
    dr.unset_name(def_)

    # Test some drudge script specials about the free algebra environment.
    assert env['DRUDGE'] is dr
    assert env['sum_'] is sum


TEST_PICKLE_DRS = """
import pickle

symb = pickle.loads(pickle.dumps(f))
good_symb = symb == f

indexed = pickle.loads(pickle.dumps(f[i, j]))
good_indexed = indexed == f[i, j]

def_ = x[i] <= einst(f[i] * v[i]) / 2
def_serial = pickle.dumps(def_)
def_back = pickle.loads(def_serial)
"""


def test_pickle_within_drs(free_alg):
    """Test pickling inside drudge scripts."""

    dr = free_alg
    env = dr.exec_drs(TEST_PICKLE_DRS)

    assert env['good_symb']
    assert env['good_indexed']
    assert env['def_'] == env['def_back']


def test_inverse_of_linear_vector_transforms(free_alg: Drudge):
    """Test automatic inversion of linear vector transformations.

    Here, we also have good coverage on the conversion of linear transformations
    in matrix and definitions forms.
    """

    dr = free_alg
    p = dr.names
    v = p.v

    a = Vec('a')
    b = Vec('b')

    defs = [
        dr.define(a, v + 1),
        dr.define(b, v - 1)
    ]
    res = dr.lvt_inv(defs)

    assert len(res) == 2
    half = Rational(1, 2)
    one_checked = False
    v_checked = False
    for i in res:
        if i.lhs == 1:
            assert (i - half * a + half * b).simplify() == 0
            one_checked = True
        elif i.lhs == v:
            assert (i - half * a - half * b).simplify() == 0
            v_checked = True
        else:
            assert False
        continue

    assert one_checked and v_checked
