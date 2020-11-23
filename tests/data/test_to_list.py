import rx
import rxsci as rs


def test_to_list():
    actual_result = []
    source = [1, 2, 3, 4]

    rx.from_(source).pipe(
        rs.data.to_list()
    ).subscribe(
        on_next=actual_result.append
    )

    assert actual_result == [[1, 2, 3, 4]]


def test_to_list_mux():
    actual_result = []
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((2,), 10),
        rs.OnNextMux((2,), 11),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((2,), 12),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.data.to_list(),
        ),
    ).subscribe(
        on_next=actual_result.append
    )

    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((1,), [1, 2, 3, 4]),
        rs.OnCompletedMux((1,)),
        rs.OnNextMux((2,), [10, 11, 12]),
        rs.OnCompletedMux((2,)),
    ]
