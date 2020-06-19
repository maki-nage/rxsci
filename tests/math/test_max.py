from pytest import approx
import rx
import rxsci as rs


def test_max_empty():
    source = []
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max()
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [None]


def test_max_int():
    source = [4, 10, 3, 2]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max()
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [4, 10, 10, 10]


def test_max_int_reduce():
    source = [4, 10, 3, 2]
    expected_result = [10]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_max_float():
    source = [2.76, 3, 10.43, 4]
    expected_result = [10.43]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert len(actual_result) == 1
    assert actual_result[0] == expected_result[0]


def test_max_key_mapper():
    source = [('a', 2), ('b', 3), ('c', 10), ('d', 4)]
    expected_result = [10]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max(lambda i: i[1], reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result
