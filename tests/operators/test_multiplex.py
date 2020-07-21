import rx
import rx.operators as ops
import rxsci as rs
import rxsci.operators as rsops


def test_multiplex():
    source = [1, 2, 3, 4]
    actual_error = []
    actual_completed = []
    actual_result = []
    mux_actual_result = []

    def on_completed():
        print("PPPPPPPPP")
        actual_completed.append(True)

    rx.from_(source).pipe(
        rsops.multiplex(
            rx.pipe(
                ops.do_action(mux_actual_result.append),
            ),
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
        rs.OnCreateMux(None),
        rs.OnNextMux(None, 1),
        rs.OnNextMux(None, 2),
        rs.OnNextMux(None, 3),
        rs.OnNextMux(None, 4),
        rs.OnCompletedMux(None),
    ]
