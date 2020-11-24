import rx
import rx.operators as ops
import rxsci as rs


def test_split():
    source = ["1a", "2a", "3b", "4b", "5c", "6c", "7c", "8d", "9d"]
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), '1a'),
        rs.OnNextMux((1, None), '2a'),
        rs.OnNextMux((1, None), '3b'),
        rs.OnNextMux((1, None), '4b'),
        rs.OnNextMux((1, None), '5c'),
        rs.OnNextMux((1, None), '6c'),
        rs.OnNextMux((1, None), '7c'),
        rs.OnNextMux((1, None), '8d'),
        rs.OnNextMux((1, None), '9d'),
        rs.OnCompletedMux((1, None)),
    ]
    actual_result = []
    mux_actual_result = []
    expected_result = [
        ["1a", "2a"],
        ["3b", "4b"],
        ["5c", "6c", "7c"],
        ["8d", "9d"],
    ]

    def on_next(i):
        actual_result.append(i)

    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_store(
            store,
            rs.data.split(lambda i: i[-1], rx.pipe(
                ops.do_action(mux_actual_result.append),
            )),
        ),
    ).subscribe(on_next)

    assert type(mux_actual_result[0]) is rs.state.ProbeStateTopology
    assert mux_actual_result[1:] == [
        rs.OnCreateMux((1, (1, None)), store),
        rs.OnNextMux((1, (1, None)), '1a', store),
        rs.OnNextMux((1, (1, None)), '2a', store),
        rs.OnCompletedMux((1, (1, None)), store),
        rs.OnCreateMux((1, (1, None)), store),
        rs.OnNextMux((1, (1, None)), '3b', store),
        rs.OnNextMux((1, (1, None)), '4b', store),
        rs.OnCompletedMux((1, (1, None)), store),
        rs.OnCreateMux((1, (1, None)), store),
        rs.OnNextMux((1, (1, None)), '5c', store),
        rs.OnNextMux((1, (1, None)), '6c', store),
        rs.OnNextMux((1, (1, None)), '7c', store),
        rs.OnCompletedMux((1, (1, None)), store),
        rs.OnCreateMux((1, (1, None)), store),
        rs.OnNextMux((1, (1, None)), '8d', store),
        rs.OnNextMux((1, (1, None)), '9d', store),
        rs.OnCompletedMux((1, (1, None)), store),
    ]
    assert actual_result == source
