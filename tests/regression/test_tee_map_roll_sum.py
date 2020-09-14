from collections import namedtuple
import rx
import rx.operators as ops
import rxsci as rs


def test_tee_map_roll_sum():
    Item = namedtuple('Item', ['group', 'a', 'b'])
    source = [
        Item('a', 0, 1000),
        Item('b', 1, 10001),
        Item('a', 1, 1001),
        Item('a', 2, 1002),
        Item('b', 2, 10002),
        Item('b', 3, 10003),
        Item('a', 3, 1003),
        Item('a', 4, 1004),
        Item('b', 4, 10004),
        Item('a', 5, 1005),                        
        Item('b', 5, 10005),
        Item('b', 6, 10006),
    ]

    actual_result = []

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.ops.multiplex(rx.pipe(
            rs.ops.group_by(lambda i: i.group, rx.pipe(
                rs.data.roll(window=3, stride=2, pipeline=rx.pipe(
                    rs.ops.tee_map(
                        rx.pipe(
                            rs.ops.map(lambda i: i.a),
                            rs.math.sum(reduce=True),
                        ),
                        rx.pipe(
                            rs.ops.map(lambda i: i.b),
                            rs.math.sum(reduce=True),
                        )
                    ),
                )),
            ))
        )),
    ).subscribe(
        on_next=on_next,
        on_error=lambda e: print(e))

    assert actual_result == [
        (3.0, 3003.0),
        (6.0, 30006.0),
        (9.0, 3009.0),        
        (12.0, 30012.0),
    ]
