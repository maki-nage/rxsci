import rx
import rx.operators as ops
import rxsci as rs


def test_multiplex_obs():
    source = [1, 2, 3, 4]
    actual_error = []
    actual_completed = []
    actual_result = []
    mux_actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.ops.multiplex(
            rx.pipe(
                ops.do_action(mux_actual_result.append),
            ),
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == source
    assert mux_actual_result == [
        rs.OnCreateMux((0,)),
        rs.OnNextMux((0,), 1),
        rs.OnNextMux((0,), 2),
        rs.OnNextMux((0,), 3),
        rs.OnNextMux((0,), 4),
        rs.OnCompletedMux((0,)),
    ]


def test_multiplex_list():
    source = [1, 2, 3, 4]
    actual_error = []
    actual_completed = []
    actual_result = []
    mux_actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.ops.multiplex(
            [
                ops.do_action(mux_actual_result.append),
            ],
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == source
    assert mux_actual_result == [
        rs.OnCreateMux((0,)),
        rs.OnNextMux((0,), 1),
        rs.OnNextMux((0,), 2),
        rs.OnNextMux((0,), 3),
        rs.OnNextMux((0,), 4),
        rs.OnCompletedMux((0,)),
    ]


def test_multiplex_forward_error_on_completion():
    source = [1, 2, 3, 4]
    actual_error = []
    actual_completed = []
    actual_result = []
    mux_actual_result = []

    def on_completed():
        actual_completed.append(True)

    def raise_exception(key):
        if key is not None:
            raise ValueError()

    rx.from_(source).pipe(
        rs.ops.multiplex(
            [
                rs.ops.do_action(
                    on_completed=raise_exception,
                ),
            ],
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert type(actual_error[0]) == ValueError
    assert actual_completed == []
    assert actual_result == source
