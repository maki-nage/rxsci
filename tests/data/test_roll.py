import rx
import rx.operators as ops
import rxsci as rs
from ..utils import on_probe_state_topology


def test_roll():
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = [
        rs.OnCreateMux((1,), store=store),
        rs.OnNextMux((1,), 1, store=store),
        rs.OnNextMux((1,), 2, store=store),
        rs.OnNextMux((1,), 3, store=store),
        rs.OnNextMux((1,), 4, store=store),
        rs.OnNextMux((1,), 5, store=store),
        rs.OnCompletedMux((1,), store=store),
    ]

    actual_result = []
    mux_actual_result = []

    def on_next(i):
        actual_result.append(i)

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
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = [
        rs.OnCreateMux((0,), store=store),
        rs.OnNextMux((0,), 1, store=store),
        rs.OnNextMux((0,), 2, store=store),
        rs.OnNextMux((0,), 3, store=store),
        rs.OnNextMux((0,), 4, store=store),
        rs.OnNextMux((0,), 5, store=store),
        rs.OnNextMux((0,), 6, store=store),
        rs.OnCompletedMux((0,), store=store),
    ]

    actual_result = []
    mux_actual_result = []

    def on_next(i):
        actual_result.append(i)

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
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = [
        rs.OnCreateMux((1,), store=store),
        rs.OnNextMux((1,), 1, store=store),
        rs.OnNextMux((1,), 2, store=store),
        rs.OnNextMux((1,), 3, store=store),
        rs.OnNextMux((1,), 4, store=store),
        rs.OnCompletedMux((1,), store=store),
    ]

    actual_result = []
    mux_actual_result = []

    def on_next(i):
        actual_result.append(i)

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


def test_roll_count_without_store():
    actual_error = []

    rx.from_([1, 2, 3, 4]).pipe(
        rs.data.roll(window=3, stride=3, pipeline=rx.pipe(
        )),
    ).subscribe(on_error=actual_error.append,)

    assert type(actual_error[0]) is ValueError


def test_roll_without_store():
    actual_error = []

    rx.from_([1, 2, 3, 4]).pipe(
        rs.data.roll(window=3, stride=2, pipeline=rx.pipe(
        )),
    ).subscribe(on_error=actual_error.append)

    assert type(actual_error[0]) is ValueError


def test_forward_topology_probe_1():
    actual_topology_probe = []
    source = [1, 2, 3, 4]

    rx.from_(source).pipe(
        rs.state.with_memory_store(
            rx.pipe(
                rs.data.roll(1, 1, pipeline=rx.pipe()),
                on_probe_state_topology(actual_topology_probe.append),
            )
        ),
    ).subscribe()

    assert len(actual_topology_probe) == 1


def test_forward_topology_probe_2():
    actual_topology_probe = []
    source = [1, 2, 3, 4]

    rx.from_(source).pipe(
        rs.state.with_memory_store(
            rx.pipe(
                rs.data.roll(2, 1, pipeline=rx.pipe()),
                on_probe_state_topology(actual_topology_probe.append),
            )
        ),
    ).subscribe()

    assert len(actual_topology_probe) == 1
