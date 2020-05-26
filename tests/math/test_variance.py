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
