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


def test_sum_completed():
    source = [2, 3, 10, 4]
    actual_completed = []

    rx.from_(source).pipe(
        rs.math.sum()
    ).subscribe(
        on_completed=lambda: actual_completed.append(True)
    )

    assert actual_completed == [True]


def test_sum_completed_on_empty():
    actual_result = []
    actual_completed = []

    rx.empty().pipe(
        rs.math.sum(reduce=True)
    ).subscribe(
        on_next=actual_result.append,
        on_completed=lambda: actual_completed.append(True)
    )

    assert actual_completed == [True]
    assert actual_result == [0]


def test_sum_mux_completed_on_empty():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnCreateMux((2,)),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]
    actual_completed = []
    actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.math.sum(reduce=True),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_error=lambda e: print(e),
        on_completed=lambda: actual_completed.append(True)
    )

    assert actual_completed == [True]
    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((1,), 0),
        rs.OnCompletedMux((1,)),
        rs.OnNextMux((2,), 0),
        rs.OnCompletedMux((2,)),
    ]


def test_sum_mux():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 2),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 8),
        rs.OnNextMux((2,), 6),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 10),
        rs.OnNextMux((1,), 4),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]

    actual_completed = []
    actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.math.sum(reduce=True),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=lambda: actual_completed.append(True)
    )

    assert actual_completed == [True]
    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((1,), 19),
        rs.OnCompletedMux((1,)),
        rs.OnNextMux((2,), 14),
        rs.OnCompletedMux((2,)),
    ]
