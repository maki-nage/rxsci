import rx
import rxsci as rs


def test_assert_ok():
    source = [1, 2, 3, 4]
    expected_result = [1, 2, 3, 4]
    actual_result = []

    rx.from_(source).pipe(
        rs.ops.assert_(lambda i: i > 0)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_assert_mux_ok():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 2),
        rs.OnNextMux((2, None), 3),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.ops.assert_(lambda i: i > 0)
    ).subscribe(on_next=actual_result.append)

    assert actual_result ==  [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 2),
        rs.OnNextMux((2, None), 3),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]


def test_assert_fail():
    source = [1, 2, 3, 4]
    expected_result = [1, 2]
    actual_result = []
    error = []

    rx.from_(source).pipe(
        rs.ops.assert_(lambda i: i < 3)
    ).subscribe(
        on_next=actual_result.append,
        on_error=error.append,
    )

    assert actual_result == expected_result
    assert type(error[0]) == ValueError


def test_assert_mux_fail():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 2),
        rs.OnNextMux((2, None), -1),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), -1),
        rs.OnCompletedMux((1, None)),
        rs.OnCompletedMux((2, None)),
    ]
    actual_result = []
    error = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.ops.assert_(lambda i: i > 0)
    ).subscribe(
        on_next=actual_result.append,
        on_error=error.append,
    )

    assert actual_result ==  [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnCreateMux((2, None)),
        rs.OnNextMux((2, None), 2),
    ]
    assert type(error[0]) == ValueError


def test_assert_1_ok():
    source = [1, 2, 3, 4]
    expected_result = [1, 2, 3, 4]
    actual_result = []

    rx.from_(source).pipe(
        rs.ops.assert_1(lambda prev, cur: cur > prev),
    ).subscribe(
        on_next=actual_result.append,        
    )

    assert actual_result == expected_result


def test_assert_1_error():
    source = [1, 2, 4, 3]
    expected_result = [1, 2, 4]
    actual_result = []
    error = []

    rx.from_(source).pipe(
        rs.ops.assert_1(lambda prev, cur: cur > prev),
    ).subscribe(
        on_next=actual_result.append,
        on_error=error.append
    )

    assert actual_result == expected_result
    assert type(error[0]) == ValueError
