from datetime import datetime, timedelta
import rx
import rx.operators as ops
import rxsci as rs
from ..utils import on_probe_state_topology


def test_time_split():
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = [
        rs.OnCreateMux((1,), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=1), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=2), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=3), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=4), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=5), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=6), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=10), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=12), store=store),
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
            rs.data.time_split(
                time_mapper=lambda i: i,
                active_timeout=timedelta(seconds=5),
                inactive_timeout=timedelta(seconds=3),
                pipeline=rx.pipe(
                    ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(on_next)

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=1), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=2), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=3), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=4), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=5), store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=6), store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=10), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=12), store),
        rs.OnCompletedMux((1, (1,)), store),
    ]
    assert actual_result == source


def test_time_split_no_active_timeout():
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = [
        rs.OnCreateMux((1,), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=1), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=2), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=3), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=4), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=5), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=6), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=10), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=12), store=store),
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
            rs.data.time_split(
                time_mapper=lambda i: i,
                inactive_timeout=timedelta(seconds=3),
                pipeline=rx.pipe(
                    ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(on_next)

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=1), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=2), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=3), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=4), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=5), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=6), store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=10), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=12), store),
        rs.OnCompletedMux((1, (1,)), store),
    ]
    assert actual_result == source


def test_time_split_no_inactive_timeout():
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = [
        rs.OnCreateMux((1,), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=1), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=2), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=3), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=4), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=5), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=6), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=10), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=12), store=store),
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
            rs.data.time_split(
                time_mapper=lambda i: i,
                active_timeout=timedelta(seconds=5),
                pipeline=rx.pipe(
                    ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(on_next)

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=1), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=2), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=3), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=4), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=5), store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=6), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=10), store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=12), store),
        rs.OnCompletedMux((1, (1,)), store),
    ]
    assert actual_result == source


def test_split_without_store():
    actual_error = []

    rx.from_([1, 2, 3, 4]).pipe(
        rs.data.time_split(
            time_mapper=lambda i: i,
            active_timeout=timedelta(seconds=5),
            inactive_timeout=timedelta(seconds=3),
            pipeline=rx.pipe()
        ),
    ).subscribe(on_error=actual_error.append)

    assert type(actual_error[0]) is ValueError


def test_forward_topology_probe():
    actual_topology_probe = []
    source = [
        datetime(2020, 1, 2, second=1)
    ]

    rx.from_(source).pipe(
        rs.state.with_memory_store(
            rx.pipe(
                rs.data.time_split(
                    time_mapper=lambda i: i,
                    active_timeout=timedelta(seconds=5),
                    inactive_timeout=timedelta(seconds=3),
                    pipeline=rx.pipe()),
                on_probe_state_topology(actual_topology_probe.append),
            )
        ),
    ).subscribe()

    assert len(actual_topology_probe) == 1


def test_closing_mapper():
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = [
        rs.OnCreateMux((1,), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=1), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=2), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=3), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=4), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=5), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=6), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=10), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=12), store=store),
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
            rs.data.time_split(
                time_mapper=lambda i: i,
                active_timeout=timedelta(seconds=5),
                inactive_timeout=timedelta(seconds=3),
                closing_mapper=lambda i: i == datetime(2020, 1, 2, second=4),
                pipeline=rx.pipe(
                    ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(on_next)

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=1), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=2), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=3), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=4), store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=5), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=6), store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=10), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=12), store),
        rs.OnCompletedMux((1, (1,)), store),
    ]
    assert actual_result == source


def test_closing_mapper_exclude():
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = [
        rs.OnCreateMux((1,), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=1), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=2), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=3), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=4), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=5), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=6), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=10), store=store),
        rs.OnNextMux((1,), datetime(2020, 1, 2, second=12), store=store),
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
            rs.data.time_split(
                time_mapper=lambda i: i,
                active_timeout=timedelta(seconds=5),
                inactive_timeout=timedelta(seconds=3),
                closing_mapper=lambda i: i == datetime(2020, 1, 2, second=4),
                include_closing_item=False,
                pipeline=rx.pipe(
                    ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(on_next)

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=1), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=2), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=3), store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=4), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=5), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=6), store),
        rs.OnCompletedMux((1, (1,)), store),
        rs.OnCreateMux((1, (1,)), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=10), store),
        rs.OnNextMux((1, (1,)), datetime(2020, 1, 2, second=12), store),
        rs.OnCompletedMux((1, (1,)), store),
    ]
    assert actual_result == source


def test_time_split_empty_source():
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    source = []
    actual_result = []
    actual_error = []
    mux_actual_result = []

    rx.from_(source).pipe(
        rs.state.with_store(
            store,
            rs.data.time_split(
                time_mapper=lambda i: i,
                active_timeout=timedelta(seconds=5),
                inactive_timeout=timedelta(seconds=3),
                closing_mapper=lambda i: i == datetime(2020, 1, 2, second=4),
                include_closing_item=False,
                pipeline=rx.pipe(
                    ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == []
    assert actual_result == source
