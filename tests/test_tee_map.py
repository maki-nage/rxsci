import rx
import rx.operators as ops
import rxsci as rs


def test_tee_map():
    source = [1, 2, 3, 4]
    actual_result = []
    expected_result = [
        (2, 1),
        (4, 2),
        (6, 3),
        (8, 4),
    ]

    rx.from_(source).pipe(
        rs.tee_map(
            lambda d: d.pipe(
                ops.map(lambda i: i*2),
            ),
            lambda d: d.pipe(
                ops.map(lambda i: i)
            )
        )
    ).subscribe(
        on_next=actual_result.append,
    )

    assert actual_result == expected_result
