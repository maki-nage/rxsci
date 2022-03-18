from pytest import approx
import numpy as np
import random

import rx
import rxsci as rs


def test_variance():
    source = [random.normalvariate(0.0, 1.0) for _ in range(10000)]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.variance(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] == approx(np.var(source), rel=1.0e-03)


def test_variance_on_empty_observable():
    source = []
    actual_result = []

    rx.from_(source).pipe(
        rs.math.variance(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] == 0.0


def test_variance_on_unit_observable():
    source = [4.2]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.variance(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] == 0.0


def test_variance_mux():
    s1 = [random.normalvariate(0.0, 1.0) for _ in range(10000)]
    s2 = [random.normalvariate(1.0, 2.0) for _ in range(10000)]
    source = [
        rs.OnCreateMux((1,)),
        rs.OnCreateMux((2,)),
    ]
    source.extend([rs.OnNextMux((1,), i) for i in s1])
    source.extend([rs.OnNextMux((2,), i) for i in s2])
    source.extend([
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),

    ])

    actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.math.variance(reduce=True),
        ),
    ).subscribe(on_next=actual_result.append)

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((1,), approx(np.var(s1), rel=1.0e-03)),
        rs.OnCompletedMux((1,)),
        rs.OnNextMux((2,), approx(np.var(s2), rel=1.0e-03)),
        rs.OnCompletedMux((2,)),
    ]
