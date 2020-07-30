import rx
import rx.operators as ops
import rxsci as rs


def test_roll():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((1, None), 4),
        rs.OnNextMux((1, None), 5),
        rs.OnCompletedMux((1, None)),
    ]

    actual_result = []

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.data.roll(3),
    ).subscribe(on_next)

    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnCreateMux((0, (1 ,None))),
        rs.OnNextMux((0, (1, None)), 1),
        rs.OnNextMux((0, (1, None)), 2),
        rs.OnNextMux((0, (1, None)), 3),        
        rs.OnCompletedMux((0, (1 ,None))),

        rs.OnCreateMux((3, (1 ,None))),
        rs.OnNextMux((3, (1, None)), 4),
        rs.OnNextMux((3, (1, None)), 5),
        #rs.OnCompletedMux((3, (1 ,None))),  # only complete windows are notified for now
        
        rs.OnCompletedMux((1, None)),
    ]


def test_roll_with_stride():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((1, None), 4),
        rs.OnNextMux((1, None), 5),
        rs.OnNextMux((1, None), 6),
        rs.OnCompletedMux((1, None)),
    ]

    actual_result = []

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.data.roll(window=3, stride=2),
    ).subscribe(on_next)

    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnCreateMux((0, (1 ,None))),
        rs.OnNextMux((0, (1, None)), 1),
        rs.OnNextMux((0, (1, None)), 2),

        rs.OnCreateMux((2, (1 ,None))),
        rs.OnNextMux((0, (1, None)), 3),
        rs.OnCompletedMux((0, (1 ,None))),
        rs.OnNextMux((2, (1, None)), 3),        
        rs.OnNextMux((2, (1, None)), 4),

        rs.OnCreateMux((4, (1 ,None))),
        rs.OnNextMux((2, (1, None)), 5),
        rs.OnCompletedMux((2, (1 ,None))),
        rs.OnNextMux((4, (1, None)), 5),
        
        rs.OnNextMux((4, (1, None)), 6),
        
        #rs.OnCompletedMux((4, (1 ,None))),  # only complete windows are notified for now
        
        rs.OnCompletedMux((1, None)),
    ]


def test_roll_identity():
    source = [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 1),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((1, None), 4),
        rs.OnCompletedMux((1, None)),
    ]

    actual_result = []

    def on_next(i):
        actual_result.append(i)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.data.roll(1),
    ).subscribe(on_next)

    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnCreateMux((0, (1 ,None))),
        rs.OnNextMux((0, (1, None)), 1),
        rs.OnCompletedMux((0, (1 ,None))),

        rs.OnCreateMux((1, (1 ,None))),
        rs.OnNextMux((1, (1, None)), 2),
        rs.OnCompletedMux((1, (1 ,None))),

        rs.OnCreateMux((2, (1 ,None))),
        rs.OnNextMux((2, (1, None)), 3),
        rs.OnCompletedMux((2, (1 ,None))),

        rs.OnCreateMux((3, (1 ,None))),
        rs.OnNextMux((3, (1, None)), 4),
        rs.OnCompletedMux((3, (1 ,None))),

        rs.OnCompletedMux((1, None)),
    ]



