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

    assert actual_result[0] is None
