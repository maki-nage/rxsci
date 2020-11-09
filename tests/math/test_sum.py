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
        rs.OnCreateMux((1 ,None)),
        rs.OnCreateMux((2, None)),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_completed = []
    actual_result = []

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.math.sum(reduce=True),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_error=lambda e: print(e),
        on_completed=lambda: actual_completed.append(True)
    )

    assert actual_completed == [True]
    assert actual_result == [
        rs.OnCreateMux((1 ,None), store),
        rs.OnCreateMux((2, None), store),
        rs.OnNextMux((1, None), 0, store),
        rs.OnCompletedMux((1, None), store),
        rs.OnNextMux((2, None), 0, store),
        rs.OnCompletedMux((2, None), store),
    ]


def test_sum_mux():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 2),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 8),
        rs.OnNextMux((2, None), 6),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((1, None), 10),
        rs.OnNextMux((1, None), 4),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]

    actual_completed = []
    actual_result = []

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.math.sum(reduce=True),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=lambda: actual_completed.append(True)
    )

    assert actual_completed == [True]
    assert actual_result == [
        rs.OnCreateMux((1 ,None), store),
        rs.OnCreateMux((2, None), store),
        rs.OnNextMux((1, None), 19, store),
        rs.OnCompletedMux((1, None), store),
        rs.OnNextMux((2, None), 14, store),
        rs.OnCompletedMux((2, None), store),
    ]
