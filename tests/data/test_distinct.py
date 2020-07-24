import rx
import rxsci as rs


def test_distinct():
    source = [1, 2, 3, 4, 1, 3, 10]
    actual_result = []

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.data.distinct()
    ).subscribe(on_next)

    assert actual_result == [1, 2, 3, 4, 10]


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

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.data.distinct(lambda i: (i[0], i[1]))
    ).subscribe(on_next)

    assert actual_result == [
        (1, "foo", 1),
        (1, "bar", 1),
        (2, "biz", 1),
        (3, "biz", 1),
        (7, "Biz", 1),
    ]


def test_distinct_mux():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 5),
        rs.OnNextMux((1, None), 3),
        rs.OnCompletedMux((1, None)),
    ]
    actual_result = []

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.data.distinct()
    ).subscribe(on_next)

    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((1, None), 5),
        rs.OnCompletedMux((1, None)),
    ]

