import rx
import rx.operators as ops
import rxsci as rs
import rxsci.operators as rsops


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
        rs.tee_map(
            rsops.first(),
            rsops.last(),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=lambda: actual_completed.append(True)
    )

    #assert actual_completed == [True]
    assert actual_result == [
        rs.OnCreateMux((1 ,None)),        
        rs.OnNextMux((1, None), (1, 2)),        
        rs.OnCompletedMux((1, None)),
    ]
