from pytest import approx
import rx
import rxsci as rs


def test_sum_int():
    source = [2, 3, 10, 4]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.sum()
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [2, 5, 15, 19]


def test_sum_int_reduce():
    source = [2, 3, 10, 4]
    expected_result = [19]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.sum(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_sum_float():
    source = [2.76, 3, 10.43, 4]
    expected_result = [20.19]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.sum(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert len(actual_result) == 1
    assert actual_result[0] == approx(expected_result[0])


def test_sum_key_mapper():
    source = [('a', 2), ('b', 3), ('c', 10), ('d', 4)]
    expected_result = [19]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.sum(lambda i: i[1], reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result
