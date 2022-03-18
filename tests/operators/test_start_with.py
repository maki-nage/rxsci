import rx
import rx.operators as ops
import rxsci as rs


def test_start_with():
    actual_result = []
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 5),
        rs.OnCompletedMux((1,)),
    ]

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.start_with([-3, -2, -1, 0]),
        ),
    ).subscribe(
        on_next=actual_result.append)

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), -3),
        rs.OnNextMux((1,), -2),
        rs.OnNextMux((1,), -1),
        rs.OnNextMux((1,), 0),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 5),
        rs.OnCompletedMux((1,)),
    ]


def test_start_with_without_store():
    actual_error = []

    rx.from_([1, 2, 3, 4]).pipe(
        rs.ops.start_with([0, -1])
    ).subscribe(
        on_error=actual_error.append,
    )

    assert type(actual_error[0]) is ValueError
