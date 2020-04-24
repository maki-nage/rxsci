import pytest
from datetime import datetime
import rx
import rxsci as rs


def test_cache_string():
    source = ["foo", "foo"]
    source[0] += "àé"
    source[1] += "àé"
    actual_result = []

    rx.from_(source).pipe(
        rs.data.cache()
    ).subscribe(on_next=actual_result.append)

    assert actual_result[0] is actual_result[1]


def test_cache_datetime():
    source = [
        datetime(year=2020, month=1, day=12),
        datetime(year=2020, month=1, day=12),
    ]
    actual_result = []

    rx.from_(source).pipe(
        rs.data.cache()
    ).subscribe(on_next=actual_result.append)

    #rx.from_(source).subscribe(on_next=actual_result.append)

    assert actual_result[0] is actual_result[1]
