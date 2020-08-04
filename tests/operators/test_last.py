import rx
import rx.operators as ops
import rxsci as rs
import rxsci.operators as rsops


def test_last_mux():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 10),
        rs.OnNextMux((2, None), 11),
        rs.OnNextMux((1, None), 2),
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
        rsops.last(),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == [
        rs.OnCreateMux((1 ,None)),        
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((1, None), 2),        
        rs.OnCompletedMux((1, None)),
        rs.OnNextMux((2, None), 11),
        rs.OnCompletedMux((2, None)),
    ]


