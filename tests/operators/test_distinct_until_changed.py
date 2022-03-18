import rx
import rxsci as rs


def test_distinct_until_changed():
    source = [1, 2, 2, 3, 1]
    actual_result = []

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.ops.distinct_until_changed()
    ).subscribe(on_next)

    assert actual_result == [1, 2, 3, 1]


def test_distinct_until_changed_with_key():
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

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.ops.distinct_until_changed(lambda i: (i[0], i[1]))
    ).subscribe(on_next)

    assert actual_result == [
        (1, "foo", 1),
        (1, "bar", 1),
        (2, "biz", 1),
        (1, "bar", 2),
        (3, "biz", 1),
        (7, "Biz", 1),
    ]


def test_distinct_until_changed_mux():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnCompletedMux((1,)),
    ]
    actual_result = []

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.distinct_until_changed()
        ),
    ).subscribe(on_next)

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnCompletedMux((1,)),
    ]
