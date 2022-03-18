import rx
import rx.operators as ops
import rxsci as rs


def test_completion():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnNextMux((1, None), 2),
        rs.OnCompletedMux((1, None)),
    ]
    actual_result = []
    actual_completed = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.tee_map(
                rs.ops.first(),
                rs.ops.last(),
            ),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=lambda: actual_completed.append(True)
    )

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1 ,None)),        
        rs.OnNextMux((1, None), (1, 2)),        
        rs.OnCompletedMux((1, None)),
    ]


def test_aggregation_completion():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnNextMux((1, None), 2),
        rs.OnCompletedMux((1, None)),
    ]
    actual_result = []
    actual_completed = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.tee_map(
                rs.ops.last(),
                rs.data.to_list(),
            ),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=lambda: actual_completed.append(True)
    )

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), (2, [1, 2])),
        rs.OnCompletedMux((1, None)),
    ]

