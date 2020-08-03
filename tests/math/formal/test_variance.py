from pytest import approx
import numpy as np
import random

import rx
import rxsci as rs


def test_variance():
    source = [random.normalvariate(0.0, 1.0) for _ in range(10)]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.formal.variance(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] == approx(np.var(source))


def test_variance_on_empty_observable():
    source = []
    actual_result = []

    rx.from_(source).pipe(
        rs.math.formal.variance(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] == 0.0


def test_variance_mux():
    s1 = [random.normalvariate(0.0, 1.0) for _ in range(10)]
    s2 = [random.normalvariate(1.0, 2.0) for _ in range(10)]
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnCreateMux((2, None)),
    ]
    source.extend([rs.OnNextMux((1, None), i) for i in s1])
    source.extend([rs.OnNextMux((2, None), i) for i in s2])
    source.extend([
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),

    ])

    actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.math.formal.variance(reduce=True),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((1, None), approx(np.var(s1))),
        rs.OnCompletedMux((1, None)),
        rs.OnNextMux((2, None), approx(np.var(s2))),
        rs.OnCompletedMux((2, None)),
    ]
