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
        rs.ops.tee_map(
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


def test_tee_map_no_zip():
    source = [1, 2, 3, 4]
    actual_result = []

    rx.from_(source).pipe(
        rs.ops.tee_map(
            lambda d: d.pipe(
                ops.map(lambda i: i*2),
            ),
            lambda d: d.pipe(
                ops.map(lambda i: i)
            ),
            zip=False
        )
    ).subscribe(
        on_next=actual_result.append,
    )

    assert actual_result == [
        2, 1,
        4, 2,
        6, 3,
        8, 4,
    ]


def test_tee_map_mux():
    source = [
        rs.OnCreateMux((1, None)),
        rs.OnNextMux((1, None), 1),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 2),
        rs.OnNextMux((2, None), 2),
        rs.OnNextMux((1, None), 1),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_result = []
    actual_error = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.ops.tee_map(
            lambda d: d.pipe(
                rs.ops.map(lambda i: i*2),
            ),
            lambda d: d.pipe(
                rs.ops.map(lambda i: i)
            )
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), (2, 1)),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), (4, 2)),
        rs.OnNextMux((2, None), (4, 2)),
        rs.OnNextMux((1, None), (2, 1)),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]


def test_tee_map_mux_no_zip():
    source = [
        rs.OnCreateMux((1, None)),
        rs.OnNextMux((1, None), 1),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 2),
        rs.OnNextMux((2, None), 2),
        rs.OnNextMux((1, None), 1),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_result = []
    actual_error = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.ops.tee_map(
            lambda d: d.pipe(
                rs.ops.map(lambda i: i*2),
            ),
            lambda d: d.pipe(
                rs.ops.map(lambda i: i)
            ),
            zip=False,
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_result == [
        rs.OnCreateMux((1, None)),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 1),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 4),
        rs.OnNextMux((2, None), 2),
        rs.OnNextMux((2, None), 4),
        rs.OnNextMux((2, None), 2),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 1),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
