import rx
import rx.operators as ops
import rxsci as rs


def test_split():
    source = ["1a", "2a", "3b", "4b", "5c", "6c", "7c", "8d", "9d"]
    actual_result = []
    expected_result = [
        ["1a", "2a"],
        ["3b", "4b"],
        ["5c", "6c", "7c"],
        ["8d", "9d"],
    ]

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.data.split(lambda i: i[-1]),
        ops.flat_map(lambda i: i.pipe(
            ops.to_list(),
        )),
    ).subscribe(on_next)

    assert actual_result == expected_result
