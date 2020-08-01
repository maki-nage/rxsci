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

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.data.split(lambda i: i[-1], rx.pipe(
            ops.do_action(mux_actual_result.append),
        )),
    ).subscribe(on_next)

    assert mux_actual_result == [
        rs.OnCreateMux(key=(1, (1, None))),
        rs.OnNextMux(key=(1, (1, None)), item='1a'),
        rs.OnNextMux(key=(1, (1, None)), item='2a'),
        rs.OnCompletedMux(key=(1, (1, None))),
        rs.OnCreateMux(key=(1, (1, None))),
        rs.OnNextMux(key=(1, (1, None)), item='3b'),
        rs.OnNextMux(key=(1, (1, None)), item='4b'),
        rs.OnCompletedMux(key=(1, (1, None))),
        rs.OnCreateMux(key=(1, (1, None))),
        rs.OnNextMux(key=(1, (1, None)), item='5c'),
        rs.OnNextMux(key=(1, (1, None)), item='6c'),
        rs.OnNextMux(key=(1, (1, None)), item='7c'),
        rs.OnCompletedMux(key=(1, (1, None))),
        rs.OnCreateMux(key=(1, (1, None))),
        rs.OnNextMux(key=(1, (1, None)), item='8d'),
        rs.OnNextMux(key=(1, (1, None)), item='9d'),
        rs.OnCompletedMux(key=(1, (1, None))),
    ]
    assert actual_result == source
