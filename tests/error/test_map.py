import rx
from rx.subject import Subject
import rx.operators as ops
import rxsci as rs


def test_map_mux_error():
    source = [1, 0, 2, 0]
    actual_result = []
    actual_error = []

    rx.from_(source).pipe(
        rs.ops.multiplex(rx.pipe(
            rs.ops.map(lambda i: 1 / i),
            rs.error.map(lambda e: 0.0),
        ))
    ).subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append
    )

    assert actual_error == []
    assert actual_result == [1.0, 0.0, 0.5, 0.0]


def test_map_mux_error_with_error():
    source = [1, 0, 2, 0]
    actual_result = []
    actual_error = []

    def _raise(e):
        raise ValueError()

    rx.from_(source).pipe(
        rs.ops.multiplex(rx.pipe(
            rs.ops.map(lambda i: 1 / i),
            rs.error.map(_raise),
        ))
    ).subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append
    )

    assert len(actual_error) == 1
    assert type(actual_error[0]) is ValueError
    assert actual_result == [1.0]
