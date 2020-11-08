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


def test_max_empty_reduce():
    source = []
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max(reduce=True)
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
    ).subscribe(
        on_next=actual_result.append,
        on_error=lambda e: print(e))

    assert actual_result == expected_result


def test_max_mux():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 4),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 8),
        rs.OnNextMux((2, None), 6),
        rs.OnNextMux((1, None), 10),
        rs.OnNextMux((1, None), 3),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.math.max(),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    try:
        assert actual_result == [
            rs.OnCreateMux((1, None), store),
            rs.OnNextMux((1, None), 4, store),
            rs.OnCreateMux((2, None), store),
            rs.OnNextMux((2, None), 8, store),
            rs.OnNextMux((2, None), 8, store),
            rs.OnNextMux((1, None), 10, store),
            rs.OnNextMux((1, None), 10, store),
            rs.OnCompletedMux((1, None), store),
            rs.OnCompletedMux((2, None), store),
        ]
    except Exception as e:
        import traceback
        traceback.print_tb(e.__traceback__)
        raise e

def test_max_mux_reduce():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 4),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 8),
        rs.OnNextMux((2, None), 6),
        rs.OnNextMux((1, None), 10),
        rs.OnNextMux((1, None), 3),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.math.max(reduce=True),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == [
        rs.OnCreateMux((1 ,None), store),
        rs.OnCreateMux((2, None), store),
        rs.OnNextMux((1, None), 10, store),
        rs.OnCompletedMux((1, None), store),
        rs.OnNextMux((2, None), 8, store),
        rs.OnCompletedMux((2, None), store),
    ]


def test_max_mux_empty_reduce():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnCreateMux((2, None)),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.math.max(reduce=True),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == [
        rs.OnCreateMux((1 ,None), store),
        rs.OnCreateMux((2, None), store),
        rs.OnNextMux((1, None), None, store),
        rs.OnCompletedMux((1, None), store),
        rs.OnNextMux((2, None), None, store),
        rs.OnCompletedMux((2, None), store),
    ]
