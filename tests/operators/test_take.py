import rx
import rx.operators as ops
import rxsci as rs


def test_take():
    source = [1, 2, 3, 4]

    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed(): actual_completed.append(True)

    rx.from_(source).pipe(
        rs.ops.take(2),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == [1, 2]


def test_take_mux():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 10),
        rs.OnNextMux((2, None), 11),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((2, None), 12),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.take(2),
        ),       
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 10),
        rs.OnNextMux((2, None), 11),
        rs.OnNextMux((1, None), 2),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
