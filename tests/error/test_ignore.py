import rx
from rx.subject import Subject
import rx.operators as ops
import rxsci as rs


def test_ignore_on_mux_error():
    source = [1, 0, 2, 0]
    actual_result = []
    actual_error = []

    rx.from_(source).pipe(
        rs.ops.multiplex(rx.pipe(
            rs.ops.map(lambda i: 1 / i),
            rs.error.ignore(),
        ))
    ).subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append
    )

    assert actual_error == []
    assert actual_result == [1.0, 0.5]
