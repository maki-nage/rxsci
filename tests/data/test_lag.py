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
        rs.state.with_memory_store(
            rs.data.lag(1),
        ),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_lag1_mux():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((1, None), 4),
        rs.OnCompletedMux((1, None)),
    ]
    actual_result = []

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.data.lag(1),
        ),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [
        rs.OnCreateMux((1 ,None), store),
        rs.OnNextMux((1, None), (1,1), store),
        rs.OnNextMux((1, None), (1,2), store),
        rs.OnNextMux((1, None), (2,3), store),
        rs.OnNextMux((1, None), (3,4), store),
        rs.OnCompletedMux((1, None), store),
    ]


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
        rs.state.with_memory_store(
            rs.data.lag(2),
        ),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_lag_mux():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((1, None), 4),
        rs.OnNextMux((1, None), 5),
        rs.OnNextMux((1, None), 6),
        rs.OnCompletedMux((1, None)),
    ]
    actual_result = []

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.data.lag(2),
        ),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [
        rs.OnCreateMux((1 ,None), store),
        rs.OnNextMux((1, None), (1, 1), store),
        rs.OnNextMux((1, None), (1, 2), store),
        rs.OnNextMux((1, None), (1, 3), store),
        rs.OnNextMux((1, None), (2, 4), store),
        rs.OnNextMux((1, None), (3, 5), store),
        rs.OnNextMux((1, None), (4, 6), store),
        rs.OnCompletedMux((1, None), store),
    ]

