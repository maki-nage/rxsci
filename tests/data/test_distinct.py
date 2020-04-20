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
