from typing import NamedTuple
import pytest
import rx
import rx.operators as ops
from rx.internal.exceptions import SequenceContainsNoElementsError

import rxsci as rs
import pandas as pd


class Item(NamedTuple):
    s1: str
    i2: int


def test_from_pandas_base():
    data = [
        Item("foo", 1),
        Item("bar", 2),
    ]

    df = pd.DataFrame(data)
    actual_result = rs.ops.from_pandas(df).pipe(
        ops.map(lambda i: i.i2),
        ops.sum(),
    ).run()

    assert actual_result == 3


def test_to_pandas_base():
    data = [
        Item("foo", 1),
        Item("bar", 2),
    ]

    expected_result = pd.DataFrame(data)
    actual_result = rx.from_(data).pipe(
        rs.ops.to_pandas()
    ).run()

    assert len(actual_result.compare(expected_result)) == 0


def test_to_pandas_on_empty_observable():
    with pytest.raises(SequenceContainsNoElementsError) as e:
        rx.from_([]).pipe(
            rs.ops.to_pandas()
        ).run()
