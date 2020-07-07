import rx
import rx.operators as ops
import rxsci as rs


def test_lag1():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    actual_result = []
    expected_result = [
        (1, 1),
        (1, 2),
        (2, 3),
        (3, 4),
        (4, 5),
        (5, 6),
        (6, 7),
        (7, 8),
        (8, 9),
    ]

    rx.from_(source).pipe(
        rs.data.lag1(),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_lag():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    actual_result = []
    expected_result = [
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 4),
        (3, 5),
        (4, 6),
        (5, 7),
        (6, 8),
        (7, 9),
    ]

    rx.from_(source).pipe(
        rs.data.lag(2),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result
