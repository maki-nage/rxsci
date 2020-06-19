from pytest import approx
import rx
import rxsci as rs


def test_min_empty():
    source = []
    actual_result = []

    rx.from_(source).pipe(
        rs.math.min()
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [None]


def test_min_int():
    source = [4, 10, 3, 2]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.min()
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [4, 4, 3, 2]


def test_min_int_reduce():
    source = [4, 10, 3, 2]
    expected_result = [2]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.min(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_min_float():
    source = [2.76, 3, 10.43, 4]
    expected_result = [2.76]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.min(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert len(actual_result) == 1
    assert actual_result[0] == expected_result[0]


def test_min_key_mapper():
    source = [('a', 2), ('b', 3), ('c', 10), ('d', 4)]
    expected_result = [2]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.min(lambda i: i[1], reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result
