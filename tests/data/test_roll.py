import rx
import rx.operators as ops
import rxsci as rs


def test_roll():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    window = 3
    actual_result = []
    expected_result = [
        [1, 2, 3],
        [2, 3, 4],
        [3, 4, 5],
        [4, 5, 6],
        [5, 6, 7],
        [6, 7, 8],
        [7, 8, 9],
    ]

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.data.roll(window),
        ops.flat_map(lambda i: i.pipe(
            ops.to_list(),
        )),
    ).subscribe(on_next)

    assert actual_result == expected_result


def test_roll_identity():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    window = 1
    actual_result = []
    expected_result = [
        [1], [2], [3], [4], [5], [6], [7], [8], [9]
    ]

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.data.roll(window),
        ops.flat_map(lambda i: i.pipe(
            ops.to_list(),
        )),
    ).subscribe(on_next)

    assert actual_result == expected_result


def test_roll_right_padding():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    window = 3
    actual_result = []
    expected_result = [
        [1, 2, 3],
        [2, 3, 4],
        [3, 4, 5],
        [4, 5, 6],
        [5, 6, 7],
        [6, 7, 8],
        [7, 8, 9],
        [8, 9, 9],
        [9, 9, 9],
    ]

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.data.roll(window, padding=rs.Padding.RIGHT),
        ops.flat_map(lambda i: i.pipe(
            ops.to_list(),
        )),
    ).subscribe(on_next)

    assert actual_result == expected_result


def test_roll_right_padding_5_2():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    window = 5
    actual_result = []
    expected_result = [
        [1, 2, 3, 4, 5],
        [3, 4, 5, 6, 7],
        [5, 6, 7, 8, 9],
        [7, 8, 9, 10, 10],
        [9, 10, 10, 10, 10],
    ]

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.data.roll(window, step=2, padding=rs.Padding.RIGHT),
        ops.flat_map(lambda i: i.pipe(
            ops.to_list(),
        )),
    ).subscribe(on_next)

    assert actual_result == expected_result


def test_roll_left_padding():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    window = 3
    actual_result = []
    expected_result = [
        [1, 1, 1],
        [1, 1, 2],
        [1, 2, 3],
        [2, 3, 4],
        [3, 4, 5],
        [4, 5, 6],
        [5, 6, 7],
        [6, 7, 8],
        [7, 8, 9],
    ]

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.data.roll(window, padding=rs.Padding.LEFT),
        ops.flat_map(lambda i: i.pipe(
            ops.to_list(),
        )),
    ).subscribe(on_next)

    assert actual_result == expected_result


def test_roll_left_padding_5_2():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    window = 5
    actual_result = []
    expected_result = [
        [1, 1, 1, 1, 1],
        [1, 1, 1, 2, 3],
        [1, 2, 3, 4, 5],
        [3, 4, 5, 6, 7],
        [5, 6, 7, 8, 9],
    ]

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.data.roll(window, step=2, padding=rs.Padding.LEFT),
        ops.flat_map(lambda i: i.pipe(
            ops.to_list(),
        )),
    ).subscribe(on_next)

    assert actual_result == expected_result
