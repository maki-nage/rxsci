import rx
import rxsci as rs


def test_assert_ok():
    source = [1, 2, 3, 4]
    expected_result = [1, 2, 3, 4]
    actual_result = []

    rx.from_(source).pipe(
        rs.assert_(lambda i: i > 0)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_assert_fail():
    source = [1, 2, 3, 4]
    expected_result = [1, 2]
    actual_result = []
    error = []

    rx.from_(source).pipe(
        rs.assert_(lambda i: i < 3)
    ).subscribe(
        on_next=actual_result.append,
        on_error=error.append,
    )

    assert actual_result == expected_result
    assert type(error[0]) == ValueError


def test_assert_scan_ok():
    source = [1, 2, 3, 4]
    expected_result = [1, 2, 3, 4]
    actual_result = []

    rx.from_(source).pipe(
        rs.assert_(
            lambda i: i[1] > i[0] if (i[0] is not None and i[1] is not None) else True,
            accumulator=lambda acc, i: (acc[1], i),
            seed=(None, None))
    ).subscribe(
        on_next=actual_result.append)

    assert actual_result == expected_result


def test_assert_scan_error():
    source = [1, 2, 4, 3]
    expected_result = [1, 2, 4]
    actual_result = []
    error = []

    rx.from_(source).pipe(
        rs.assert_(
            lambda i: i[1] > i[0] if (i[0] is not None and i[1] is not None) else True,
            accumulator=lambda acc, i: (acc[1], i),
            seed=(None, None))
    ).subscribe(
        on_next=actual_result.append,
        on_error=error.append
    )

    assert actual_result == expected_result
    assert type(error[0]) == ValueError
