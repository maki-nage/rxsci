import rx
import rx.operators as ops
import rxsci as rs


def test_roll():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 5),
        rs.OnCompletedMux((1,)),
    ]

    actual_result = []
    mux_actual_result = []

    def on_next(i):
        actual_result.append(i)

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.data.roll(window=3, stride=3, pipeline=rx.pipe(
                ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(on_next)

    assert actual_result == source
    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), 1, store),
        rs.OnNextMux((1, (1,)), 2, store),
        rs.OnNextMux((1, (1,)), 3, store),
        rs.OnCompletedMux((1, (1,)), store),

        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), 4, store),
        rs.OnNextMux((1, (1,)), 5, store),
        rs.OnCompletedMux((1, (1,)), store),
    ]


def test_roll_with_stride():
    source = [
        rs.OnCreateMux((0 ,)),
        rs.OnNextMux((0,), 1),
        rs.OnNextMux((0,), 2),
        rs.OnNextMux((0,), 3),
        rs.OnNextMux((0,), 4),
        rs.OnNextMux((0,), 5),
        rs.OnNextMux((0,), 6),
        rs.OnCompletedMux((0,)),
    ]

    actual_result = []
    mux_actual_result = []

    def on_next(i):
        actual_result.append(i)

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.data.roll(window=3, stride=2, pipeline=rx.pipe(
                ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(on_next)

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((0, (0,)), store),
        rs.OnNextMux((0, (0,)), 1, store),
        rs.OnNextMux((0, (0,)), 2, store),

        rs.OnCreateMux((1, (0,)), store),
        rs.OnNextMux((0, (0,)), 3, store),
        rs.OnCompletedMux((0, (0,)), store),
        rs.OnNextMux((1, (0,)), 3, store),
        rs.OnNextMux((1, (0,)), 4, store),

        rs.OnCreateMux((0, (0,)), store),
        rs.OnNextMux((0, (0,)), 5, store),
        rs.OnNextMux((1, (0,)), 5, store),
        rs.OnCompletedMux((1, (0,)), store),

        rs.OnNextMux((0, (0,)), 6, store),

        rs.OnCompletedMux((0, (0,)), store),
    ]


def test_roll_identity():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnCompletedMux((1,)),
    ]

    actual_result = []
    mux_actual_result = []

    def on_next(i):
        actual_result.append(i)

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.data.roll(1, 1, pipeline=rx.pipe(
                ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(on_next)

    assert actual_result == source
    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), 1, store),
        rs.OnCompletedMux((1, (1,)), store),

        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), 2, store),
        rs.OnCompletedMux((1, (1,)), store),

        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), 3, store),
        rs.OnCompletedMux((1, (1,)), store),

        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), 4, store),
        rs.OnCompletedMux((1, (1,)), store),
    ]



