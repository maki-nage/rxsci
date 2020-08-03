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


def test_describe_mux():
    s1 = [random.normalvariate(0.0, 1.0) for _ in range(200)]
    s2 = [random.normalvariate(1.0, 2.0) for _ in range(200)]

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
        rs.math.dist.update(reduce=True),
        rs.math.dist.describe(),
    ).subscribe(on_next=actual_result.append)

    #assert actual_result == []
