import rx
import rx.operators as ops
import rxsci as rs
from ..utils import on_probe_state_topology


def test_group_by_obs():
    source = [1, 2, 2, 1]
    actual_error = []
    actual_completed = []
    actual_result = []
    mux_actual_result = []

    def on_completed():
        actual_completed.append(True)

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.state.with_store(
            store,
            rx.pipe(            
                rs.ops.group_by(
                    lambda i: i,
                    rx.pipe(
                        ops.do_action(mux_actual_result.append),
                    ),
            ))
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == source

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [        
        rs.OnCreateMux((0 ,(0,)), store),
        rs.OnNextMux((0, (0,)), 1, store),
        rs.OnCreateMux((1, (0,)), store),
        rs.OnNextMux((1, (0,)), 2, store),
        rs.OnNextMux((1, (0,)), 2, store),
        rs.OnNextMux((0, (0,)), 1, store),
        rs.OnCompletedMux((0, (0,)), store),
        rs.OnCompletedMux((1, (0,)), store),
    ]


def test_group_by_list():
    source = [1, 2, 2, 1]
    actual_error = []
    actual_completed = []
    actual_result = []
    mux_actual_result = []

    def on_completed():
        actual_completed.append(True)

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.state.with_store(
            store,
            rx.pipe(            
                rs.ops.group_by(
                    lambda i: i,
                    [
                        ops.do_action(mux_actual_result.append),
                    ],
            ))
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == source

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [        
        rs.OnCreateMux((0 ,(0,)), store),
        rs.OnNextMux((0, (0,)), 1, store),
        rs.OnCreateMux((1, (0,)), store),
        rs.OnNextMux((1, (0,)), 2, store),
        rs.OnNextMux((1, (0,)), 2, store),
        rs.OnNextMux((0, (0,)), 1, store),
        rs.OnCompletedMux((0, (0,)), store),
        rs.OnCompletedMux((1, (0,)), store),
    ]


def test_group_by_without_store():
    actual_error = []

    rx.from_([1, 2, 3, 4]).pipe(
        rs.ops.group_by(
            lambda i: i % 2 == 0,
            pipeline=rx.pipe(
            )
        )
    ).subscribe(on_error=actual_error.append)

    assert type(actual_error[0]) is ValueError


def test_forward_topology_probe():
    actual_topology_probe = []
    source = [1, 2, 3, 4]

    rx.from_(source).pipe(
        rs.state.with_memory_store(
            rx.pipe(
                rs.ops.group_by(
                    lambda i: i % 2 == 0,
                    pipeline=rx.pipe()
                ),
                on_probe_state_topology(actual_topology_probe.append),
            )
        ),
    ).subscribe()

    assert len(actual_topology_probe) == 1


def test_empty_source():
    source = []
    actual_result = []
    on_completed = []
    actual_error = []

    rx.from_(source).pipe(
        rs.state.with_memory_store(
            rx.pipe(
                rs.ops.group_by(
                    lambda i: i % 2 == 0,
                    pipeline=[]
                ),
            )
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=lambda: on_completed.append(True),
        on_error=actual_error.append,
    )

    assert actual_result == []
