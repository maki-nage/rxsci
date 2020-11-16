import rx
import rx.operators as ops
import rxsci as rs


def test_group_by():
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
