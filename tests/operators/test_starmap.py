import rx
import rx.operators as ops
import rxsci as rs


def test_starmap():
    source = [
        (1, 3),
        (2, 5),
        (2, 2),
        (1, 1),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.ops.starmap(lambda i, j: i+j),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == [4, 7, 4, 2]

def test_starmap_mux():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), (1, 3)),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), (2, 5)),
        rs.OnNextMux((2, None), (2, 2)),
        rs.OnNextMux((1, None), (1, 1)),
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
        rs.ops.starmap(lambda i, j: i+j),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 4),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 7),
        rs.OnNextMux((2, None), 4),
        rs.OnNextMux((1, None), 2),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
