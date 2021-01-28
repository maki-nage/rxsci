import rx
from rx.subject import Subject
import rx.operators as ops
import rxsci as rs


def test_router_on_mux_error():
    source = [1, 0, 2, 0]
    actual_result = []
    actual_error = []
    errors_result = []
    errors_error = []

    errors, route_errors = rs.error.create_error_router()

    data = rx.from_(source).pipe(
        rs.ops.multiplex(rx.pipe(
            rs.ops.map(lambda i: 1 / i),
            route_errors(),
        ))
    )

    errors.subscribe(
        on_next=errors_result.append,
        on_error=errors_error.append
    )
    data.subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append
    )

    assert errors_error == []
    assert len(errors_result) == 2
    assert type(errors_result[0]) is ZeroDivisionError
    assert type(errors_result[1]) is ZeroDivisionError
    assert actual_error == []
    assert actual_result == [1.0, 0.5]


def test_unsubscribed_dead_letter_on_mux_error():
    source = [1, 0, 2, 0]
    actual_result = []
    actual_error = []

    _, route_errors = rs.error.create_error_router()

    data = rx.from_(source).pipe(
        rs.ops.multiplex(rx.pipe(
            rs.ops.map(lambda i: 1 / i),
            route_errors(),
        ))
    )

    data.subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append
    )

    assert len(actual_error) == 1
    assert type(actual_error[0]) is ZeroDivisionError
    assert actual_result == [1.0]


def test_dispose_dead_letter():
    source = Subject()
    actual_result = []
    actual_error = []
    errors_result = []
    errors_error = []

    errors, route_errors = rs.error.create_error_router()

    data = source.pipe(
        rs.ops.multiplex(rx.pipe(
            rs.ops.map(lambda i: 1 / i),
            route_errors(),
        ))
    )

    errors_disposable = errors.subscribe(
        on_next=errors_result.append,
        on_error=errors_error.append
    )
    disposable = data.subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append
    )

    source.on_next(1)
    source.on_next(0)
    source.on_next(2)
    source.on_next(0)

    assert errors_error == []
    assert len(errors_result) == 2
    assert type(errors_result[0]) is ZeroDivisionError
    assert type(errors_result[1]) is ZeroDivisionError
    assert actual_error == []
    assert actual_result == [1.0, 0.5]

    errors_disposable.dispose()
    source.on_next(1)
    source.on_next(0)

    assert len(actual_error) == 1
    assert type(actual_error[0]) is ZeroDivisionError
    assert actual_result == [1.0, 0.5, 1.0]


def test_dead_letter_on_error():
    source = [1, 0, 2, 0]
    actual_result = []
    actual_error = []
    errors_result = []
    errors_error = []

    errors, route_errors = rs.error.create_error_router()

    data = rx.from_(source).pipe(
        rs.ops.map(lambda i: 1 / i),
        rs.ops.multiplex(rx.pipe(
            route_errors(),
        ))
    )

    errors.subscribe(
        on_next=errors_result.append,
        on_error=errors_error.append
    )
    data.subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append
    )

    assert errors_error == []
    assert len(errors_result) == 1
    assert type(errors_result[0]) is ZeroDivisionError
    assert len(actual_error) == 1
    assert actual_result == [1.0]


def test_dead_letter_multi_instance():
    source = [1, 0, 2, 4, 0]
    actual_result = []
    actual_error = []
    errors_result = []
    errors_error = []

    errors, route_errors = rs.error.create_error_router()

    data = rx.from_(source).pipe(
        rs.ops.multiplex(rx.pipe(
            rs.ops.map(lambda i: 1 / i),
            route_errors(),
            rs.ops.map(lambda i:  i / (i - 0.5)),
            route_errors(),
        ))
    )

    errors.subscribe(
        on_next=errors_result.append,
        on_error=errors_error.append
    )
    data.subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append
    )

    assert errors_error == []
    assert len(errors_result) == 3
    for e in errors_result:
        assert type(e) is ZeroDivisionError
    assert actual_error == []
    assert actual_result == [2.0, -1.0]
