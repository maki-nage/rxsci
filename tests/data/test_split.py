import rx
import rx.operators as ops
import rxsci as rs
from ..utils import on_probe_state_topology


def test_split_obs():
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = ["1a", "2a", "3b", "4b", "5c", "6c", "7c", "8d", "9d"]
    source = [
        rs.OnCreateMux((1,), store=store),
        rs.OnNextMux((1,), '1a', store=store),
        rs.OnNextMux((1,), '2a', store=store),
        rs.OnNextMux((1,), '3b', store=store),
        rs.OnNextMux((1,), '4b', store=store),
        rs.OnNextMux((1,), '5c', store=store),
        rs.OnNextMux((1,), '6c', store=store),
        rs.OnNextMux((1,), '7c', store=store),
        rs.OnNextMux((1,), '8d', store=store),
        rs.OnNextMux((1,), '9d', store=store),
        rs.OnCompletedMux((1,), store=store),
    ]
    actual_result = []
    mux_actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.data.split(lambda i: i[-1], rx.pipe(
                ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(on_next=actual_result.append)

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), '1a', store),
        rs.OnNextMux((1, (1,)), '2a', store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), '3b', store),
        rs.OnNextMux((1, (1,)), '4b', store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), '5c', store),
        rs.OnNextMux((1, (1,)), '6c', store),
        rs.OnNextMux((1, (1,)), '7c', store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), '8d', store),
        rs.OnNextMux((1, (1,)), '9d', store),
        rs.OnCompletedMux((1, (1,)), store),
    ]
    assert actual_result == source


def test_split_list():
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = ["1a", "2a", "3b", "4b", "5c", "6c", "7c", "8d", "9d"]
    source = [
        rs.OnCreateMux((1,), store=store),
        rs.OnNextMux((1,), '1a', store=store),
        rs.OnNextMux((1,), '2a', store=store),
        rs.OnNextMux((1,), '3b', store=store),
        rs.OnNextMux((1,), '4b', store=store),
        rs.OnNextMux((1,), '5c', store=store),
        rs.OnNextMux((1,), '6c', store=store),
        rs.OnNextMux((1,), '7c', store=store),
        rs.OnNextMux((1,), '8d', store=store),
        rs.OnNextMux((1,), '9d', store=store),
        rs.OnCompletedMux((1,), store=store),
    ]
    actual_result = []
    mux_actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.data.split(lambda i: i[-1], [
                ops.do_action(mux_actual_result.append),
            ]),
        ),
    ).subscribe(on_next=actual_result.append)

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), '1a', store),
        rs.OnNextMux((1, (1,)), '2a', store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), '3b', store),
        rs.OnNextMux((1, (1,)), '4b', store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), '5c', store),
        rs.OnNextMux((1, (1,)), '6c', store),
        rs.OnNextMux((1, (1,)), '7c', store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), '8d', store),
        rs.OnNextMux((1, (1,)), '9d', store),
        rs.OnCompletedMux((1, (1,)), store),
    ]
    assert actual_result == source


def test_split_without_store():
    actual_error = []

    rx.from_([1, 2, 3, 4]).pipe(
        rs.data.split(lambda i: i[-1], rx.pipe()),
    ).subscribe(on_error=actual_error.append)

    assert type(actual_error[0]) is ValueError


def test_forward_topology_probe():
    actual_topology_probe = []
    source = [1, 2, 3, 4]

    rx.from_(source).pipe(
        rs.state.with_memory_store(
            rx.pipe(
                rs.data.split(lambda i: 1, rx.pipe()),
                on_probe_state_topology(actual_topology_probe.append),
            )
        ),
    ).subscribe()

    assert len(actual_topology_probe) == 1
