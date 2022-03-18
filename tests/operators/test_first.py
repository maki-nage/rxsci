import rx
import rx.operators as ops
import rxsci as rs


def test_first_mux():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 10),
        rs.OnNextMux((2,), 11),
        rs.OnNextMux((1,), 2),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.first(),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 10),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]


