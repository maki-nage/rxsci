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

    rx.from_(source).pipe(
        rs.ops.multiplex(rx.pipe(            
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
    assert mux_actual_result == [
        rs.OnCreateMux((0 ,(0,))),
        rs.OnNextMux((0, (0,)), 1),
        rs.OnCreateMux((1, (0,))),
        rs.OnNextMux((1, (0,)), 2),
        rs.OnNextMux((1, (0,)), 2),
        rs.OnNextMux((0, (0,)), 1),
        rs.OnCompletedMux((0, (0,))),
        rs.OnCompletedMux((1, (0,))),
    ]
