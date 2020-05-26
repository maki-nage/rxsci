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
