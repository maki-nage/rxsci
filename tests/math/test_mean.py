from pytest import approx
import rx
import rxsci as rs


def test_mean_int():
    source = [2, 3, 10, 4]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.mean()
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [2, 2.5, 5, 4.75]


def test_mean_int_reduce():
    source = [2, 3, 10, 4]
    expected_result = [4.75]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.mean(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_mean_float():
    source = [2.76, 3, 10.43, 4]
    expected_result = [5.0475]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.mean(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert len(actual_result) == 1
    assert actual_result[0] == approx(expected_result[0])


def test_mean_key_mapper():
    source = [('a', 2), ('b', 3), ('c', 10), ('d', 4)]
    expected_result = [4.75]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.mean(lambda i: i[1], reduce=True)
    ).subscribe(
        on_next=actual_result.append,
        on_error=lambda e: print(e),
    )

    assert actual_result == expected_result


def test_mean_mux_key_mapper():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), ('a', 2)),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), ('A', 3)),
        rs.OnNextMux((2,), ('B', 6)),
        rs.OnNextMux((1,), ('b', 3)),
        rs.OnNextMux((1,), ('c', 10)),
        rs.OnNextMux((1,), ('d', 4)),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]
    actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.math.mean(lambda i: i[1], reduce=True),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_error=lambda e: print(e),
    )

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((1,), 4.75),
        rs.OnCompletedMux((1,)),
        rs.OnNextMux((2,), 4.5),
        rs.OnCompletedMux((2,)),
    ]
