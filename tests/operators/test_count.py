import rx
import rx.operators as ops
import rxsci as rs


def test_count():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 0),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 0),
        rs.OnNextMux((2,), 0),
        rs.OnNextMux((1,), 0),
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
            rs.ops.count(),
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
        rs.OnNextMux((2,), 1),
        rs.OnNextMux((2,), 2),
        rs.OnNextMux((1,), 2),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]


def test_count_reduce():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 0),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 0),
        rs.OnNextMux((2,), 0),
        rs.OnNextMux((1,), 0),
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
            rs.ops.count(reduce=True),
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
        rs.OnCreateMux((2,)),
        rs.OnNextMux((1,), 2),
        rs.OnCompletedMux((1,)),
        rs.OnNextMux((2,), 2),
        rs.OnCompletedMux((2,)),
    ]
