import rx
import rx.operators as ops
import rxsci as rs


def test_groupby_roll_sum():
    source = [
        ('a', 1),
        ('a', 2),
        ('b', 10),
        ('a', 3),
        ('b', 20),
        ('b', 30),
        ('a', 4),
        ('b', 40),
        ('a', 5),
        ('a', 6),
        ('b', 50),
        ('a', 7),
        ('a', 8),
        ('a', 9),
        ('a', 10),
    ]

    actual_result = []

    def on_next(i):
        actual_result.append(i)

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.state.with_store(
            store,
            rx.pipe(
                rs.ops.group_by(lambda i: i[0], rx.pipe(
                    rs.data.roll(window=3, stride=2, pipeline=rx.pipe(
                        rs.ops.tee_map(
                            rx.pipe(
                                rs.ops.map(lambda i: i[0]),
                            ),
                            rx.pipe(
                                rs.ops.map(lambda i: i[1]),
                                rs.math.sum(reduce=True),
                            )
                        ),
                    )),
                ))
            )
        ),
    ).subscribe(
        on_next=on_next,
        on_error=lambda e: print(e))

    assert actual_result == [
        ('a', 6.0),
        ('b', 60.0),
        ('a', 12.0),
        ('b', 120.0),
        ('a', 18.0),
        ('a', 24.0),
        ('a', 19.0),
        ('b', 50.0),
    ]
