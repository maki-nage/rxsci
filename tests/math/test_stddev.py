from pytest import approx
import numpy as np
import random

import rx
import rxsci as rs


def test_stddev():
    source = [random.normalvariate(0.0, 1.0) for _ in range(10000)]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.stddev(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] == approx(np.std(source), rel=1.0e-04)


def test_stddev_on_empty_observable():
    source = []
    actual_result = []

    rx.from_(source).pipe(
        rs.math.stddev(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] is None


def test_stddev_on_unit_observable():
    source = [4.2]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.stddev(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] is None
