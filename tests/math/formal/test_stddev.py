from pytest import approx
import numpy as np
import random

import rx
import rxsci as rs


def test_stddev():
    source = [random.normalvariate(0.0, 1.0) for _ in range(10)]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.formal.stddev(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] == approx(np.std(source))


def test_stddev_on_empty_observable():
    source = []
    actual_result = []

    rx.from_(source).pipe(
        rs.math.formal.stddev(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] == 0.0


def test_stddev_completed():
    source = [random.normalvariate(0.0, 1.0) for _ in range(10)]
    actual_completed = []

    rx.from_(source).pipe(
        rs.math.formal.stddev()
    ).subscribe(
        on_completed=lambda: actual_completed.append(True)
    )

    assert actual_completed == [True]


def test_stddev_completed_on_one():
    source = [0.1]
    actual_result = []
    actual_completed = []

    rx.from_(source).pipe(
        rs.math.formal.stddev()
    ).subscribe(
        on_next=actual_result.append,
        on_completed=lambda: actual_completed.append(True)
    )

    assert actual_completed == [True]
    assert actual_result == [0.0]



def test_stddev_completed_on_empty():
    actual_result = []
    actual_completed = []

    rx.empty().pipe(
        rs.math.formal.stddev(reduce=True)
    ).subscribe(
        on_next=actual_result.append,
        on_completed=lambda: actual_completed.append(True)
    )

    assert actual_completed == [True]
    assert actual_result == [0.0]
