import rx
import rx.operators as ops
import rxsci as rs


def test_pad_start_without_store():
    actual_error = []

    rx.from_([1, 2, 3, 4]).pipe(
        rs.data.pad_start(size=3, value=0),
    ).subscribe(
        on_error=actual_error.append)

    assert type(actual_error[0]) is ValueError


def test_pad_start_empty():
    actual_result = []
    source = [
        rs.OnCreateMux((1,)),
        rs.OnCompletedMux((1,)),
    ]

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.data.pad_start(size=3, value=0),
        ),
    ).subscribe(
        on_next=actual_result.append)

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnCompletedMux((1,)),
    ]


def test_pad_start():
    actual_result = []
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 5),
        rs.OnCompletedMux((1,)),
    ]

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.data.pad_start(size=3, value=0),
        ),
    ).subscribe(
        on_next=actual_result.append)

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 0),
        rs.OnNextMux((1,), 0),
        rs.OnNextMux((1,), 0),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 5),
        rs.OnCompletedMux((1,)),
    ]


def test_pad_start_no_value():
    actual_result = []
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 5),
        rs.OnCompletedMux((1,)),
    ]

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.data.pad_start(size=3),
        ),
    ).subscribe(
        on_next=actual_result.append)

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 5),
        rs.OnCompletedMux((1,)),
    ]


def test_pad_end_without_store():
    actual_error = []

    rx.from_([1, 2, 3, 4]).pipe(
        rs.data.pad_end(size=3, value=0),
    ).subscribe(on_error=actual_error.append)

    assert type(actual_error[0]) is ValueError


def test_pad_end_empty():
    actual_result = []
    source = [
        rs.OnCreateMux((1,)),
        rs.OnCompletedMux((1,)),
    ]

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.data.pad_end(size=3, value=0),
        ),
    ).subscribe(
        on_next=actual_result.append)

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnCompletedMux((1,)),
    ]


def test_pad_end():
    actual_result = []
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnCompletedMux((1,)),
    ]

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.data.pad_end(size=3, value=0),
        ),
    ).subscribe(
        on_next=actual_result.append)

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 0),
        rs.OnNextMux((1,), 0),
        rs.OnNextMux((1,), 0),
        rs.OnCompletedMux((1,)),
    ]


def test_pad_end_no_value():
    actual_result = []
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnCompletedMux((1,)),
    ]

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.data.pad_end(size=3),
        ),
    ).subscribe(
        on_next=actual_result.append)

    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 3),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 4),
        rs.OnNextMux((1,), 4),
        rs.OnCompletedMux((1,)),
    ]
