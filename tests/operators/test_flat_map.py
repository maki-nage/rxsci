import rx
import rxsci as rs


def test_flat_map_list():
    source = [1, 2, 3, 4]

    actual_result = []
    rx.just(source).pipe(
        rs.ops.flat_map(),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [1,2,3,4]


def test_flat_map_tuple():
    source = (1, 2, 3, 4)

    actual_result = []
    rx.just(source).pipe(
        rs.ops.flat_map(),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [1,2,3,4]


def test_flat_map_mux():
    source = [
        rs.OnCreateMux((1 ,)),
        rs.OnNextMux((1, ), [1, 2, 3, 4]),
        rs.OnCreateMux((2, )),
        rs.OnNextMux((2, ), [10, 11, 12, 13]),
        rs.OnCompletedMux((1, )),
        rs.OnCompletedMux((2, )),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.ops.flat_map(),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == [
        rs.OnCreateMux((1 ,)),
        rs.OnNextMux((1, ), 1),
        rs.OnNextMux((1, ), 2),
        rs.OnNextMux((1, ), 3),
        rs.OnNextMux((1, ), 4),
        rs.OnCreateMux((2, )),
        rs.OnNextMux((2, ), 10),
        rs.OnNextMux((2, ), 11),
        rs.OnNextMux((2, ), 12),
        rs.OnNextMux((2, ), 13),
        rs.OnCompletedMux((1, )),
        rs.OnCompletedMux((2, )),
    ]
