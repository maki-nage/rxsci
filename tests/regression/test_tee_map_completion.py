import rx
import rx.operators as ops
import rxsci as rs


def test_completion():
    data = [1, 2, 3]
    actual_data = []
    actual_completed = []

    rx.from_(data).pipe(
        rs.ops.tee_map(
            ops.count(),
            rs.math.sum(reduce=True),
        )
    ).subscribe(
        on_next=actual_data.append,
        on_completed=lambda: actual_completed.append(True)
    )

    assert actual_completed == [True]
    assert actual_data == [(3, 6)]
