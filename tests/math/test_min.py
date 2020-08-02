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


def test_min_mux_reduce():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 4),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 8),
        rs.OnNextMux((2, None), 6),
        rs.OnNextMux((1, None), 10),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((1, None), 2),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.math.min(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((1, None), 2),
        rs.OnCompletedMux((1, None)),
        rs.OnNextMux((2, None), 6),
        rs.OnCompletedMux((2, None)),
    ]


def test_min_mux_empty_reduce():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnCreateMux((2, None)),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.math.min(reduce=True),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((1, None), None),
        rs.OnCompletedMux((1, None)),
        rs.OnNextMux((2, None), None),
        rs.OnCompletedMux((2, None)),
    ]
