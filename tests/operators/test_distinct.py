import rx
import rxsci as rs


def test_distinct():
    source = [1, 2, 3, 4, 1, 3, 10]
    actual_result = []

    rx.from_(source).pipe(
        rs.state.with_memory_store(
            rs.ops.distinct(),
        ),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [1, 2, 3, 4, 10]


def test_distinct_without_store():
    source = [1, 2, 3, 4, 1, 3, 10]
    actual_error = []


    rx.from_(source).pipe(
        rs.ops.distinct(),
    ).subscribe(on_error=actual_error.append)

    assert type(actual_error[0]) is ValueError


def test_distinct_with_key():
    source = [
        (1, "foo", 1),
        (1, "foo", 3),
        (1, "bar", 1),
        (2, "biz", 1),
        (1, "bar", 2),
        (3, "biz", 1),
        (7, "Biz", 1),
    ]
    actual_result = []

    rx.from_(source).pipe(
        rs.state.with_memory_store(
            rs.ops.distinct(lambda i: (i[0], i[1])),
        ),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [
        (1, "foo", 1),
        (1, "bar", 1),
        (2, "biz", 1),
        (3, "biz", 1),
        (7, "Biz", 1),
    ]


def test_distinct_mux():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 5),
        rs.OnNextMux((1,), 3),
        rs.OnCompletedMux((1,)),
    ]
    actual_result = []

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.distinct(),
        ),
    ).subscribe(on_next=actual_result.append)

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 5),
        rs.OnCompletedMux((1,)),
    ]
