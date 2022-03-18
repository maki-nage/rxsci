from pytest import approx
import rx
import rxsci as rs


def test_max_empty():
    source = []
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max()
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [None]


def test_max_empty_reduce():
    source = []
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [None]


def test_max_int():
    source = [4, 10, 3, 2]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max()
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [4, 10, 10, 10]


def test_max_int_reduce():
    source = [4, 10, 3, 2]
    expected_result = [10]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_max_float():
    source = [2.76, 3, 10.43, 4]
    expected_result = [10.43]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max(reduce=True)
    ).subscribe(on_next=actual_result.append)

    assert len(actual_result) == 1
    assert actual_result[0] == expected_result[0]


def test_max_key_mapper():
    source = [('a', 2), ('b', 3), ('c', 10), ('d', 4)]
    expected_result = [10]
    actual_result = []

    rx.from_(source).pipe(
        rs.math.max(lambda i: i[1], reduce=True)
    ).subscribe(
        on_next=actual_result.append,
        on_error=lambda e: print(e))

    assert actual_result == expected_result


def test_max_mux():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 4),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 8),
        rs.OnNextMux((2,), 6),
        rs.OnNextMux((1,), 10),
        rs.OnNextMux((1,), 3),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.math.max(),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    try:
        actual_result = [r._replace(store=None) for r in actual_result]
        assert actual_result == [
            rs.OnCreateMux((1,)),
            rs.OnNextMux((1,), 4),
            rs.OnCreateMux((2,)),
            rs.OnNextMux((2,), 8),
            rs.OnNextMux((2,), 8),
            rs.OnNextMux((1,), 10),
            rs.OnNextMux((1,), 10),
            rs.OnCompletedMux((1,)),
            rs.OnCompletedMux((2,)),
        ]
    except Exception as e:
        import traceback
        traceback.print_tb(e.__traceback__)
        raise e


def test_max_mux_reduce():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 4),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 8),
        rs.OnNextMux((2,), 6),
        rs.OnNextMux((1,), 10),
        rs.OnNextMux((1,), 3),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.math.max(reduce=True),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((1,), 10),
        rs.OnCompletedMux((1,)),
        rs.OnNextMux((2,), 8),
        rs.OnCompletedMux((2,)),
    ]


def test_max_mux_empty_reduce():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnCreateMux((2,)),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.math.max(reduce=True),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((1,), None),
        rs.OnCompletedMux((1,)),
        rs.OnNextMux((2,), None),
        rs.OnCompletedMux((2,)),
    ]
