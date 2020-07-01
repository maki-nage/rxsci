from pytest import approx
import random

import rx
import rx.operators as ops
import rxsci as rs


def test_update():
    source = [random.normalvariate(0.0, 1.0) for _ in range(10)]

    # base usage
    actual_result = []
    rx.from_(source).pipe(
        rs.math.dist.update()
    ).subscribe(on_next=actual_result.append)

    assert len(actual_result) == len(source)

    # custom bins
    actual_result = []
    rx.from_(source).pipe(
        rs.math.dist.update(bin_count=64)
    ).subscribe(on_next=actual_result.append)

    assert len(actual_result) == len(source)


    # weighted diff
    actual_result = []
    rx.from_(source).pipe(
        rs.math.dist.update(weighted_diff=True)
    ).subscribe(on_next=actual_result.append)

    assert len(actual_result) == len(source)


def test_describe():
    source = [random.normalvariate(0.0, 1.0) for _ in range(200)]

    actual_result = []
    rx.from_(source).pipe(
        rs.math.dist.update(),
        ops.last(),
        rs.math.dist.describe(),
    ).subscribe(on_next=actual_result.append)
