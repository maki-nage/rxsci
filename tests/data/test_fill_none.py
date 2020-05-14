from collections import namedtuple
import rx
import rxsci as rs

x = namedtuple('x', ['foo', 'bar', 'biz'])


def test_fill_none_namedtuple():
    source = [
        x(None, 2, 3),
        x(None, None, None),
        x(1, None, None),
        x(1, 2, None),
    ]
    expected_result = [
        x(0, 2, 3),
        x(0, 0, 0),
        x(1, 0, 0),
        x(1, 2, 0),
    ]
    actual_result = []

    rx.from_(source).pipe(
        rs.data.fill_none(0)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_fill_none_value():
    source = [
        None,
        1.2,
        5.348,
        None,
    ]
    expected_result = [
        0, 1.2, 5.348, 0
    ]
    actual_result = []

    rx.from_(source).pipe(
        rs.data.fill_none(0)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result
