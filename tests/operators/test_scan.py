import pytest
import rx
import rx.operators as ops
import rxsci as rs


def test_scan_mux():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 2),
        rs.OnNextMux((2,), 2),
        rs.OnNextMux((1,), 1),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.scan(lambda acc, i: i+acc, seed=0),
        )
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 2),
        rs.OnNextMux((2,), 4),
        rs.OnNextMux((1,), 2),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]


def test_scan_mux_reduce():
    source = [
        rs.OnCreateMux((0,)),
        rs.OnNextMux((0,), 1),
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((0,), 1),
        rs.OnCompletedMux((0,)),
        rs.OnCompletedMux((1,)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.scan(lambda acc, i: i+acc, seed=0, reduce=True),
        )
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((0,)),
        rs.OnCreateMux((1,)),
        rs.OnNextMux((0,), 2),
        rs.OnCompletedMux((0,)),
        rs.OnNextMux((1,), 4),
        rs.OnCompletedMux((1,)),
    ]


def test_scan_mux_reduce_empty():
    source = [
        rs.OnCreateMux((0,)),
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 2),
        rs.OnCompletedMux((0,)),
        rs.OnCompletedMux((1,)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.scan(lambda acc, i: i+acc, seed=0, reduce=True),
        )
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((0,)),
        rs.OnCreateMux((1,)),
        rs.OnNextMux((0,), 0),
        rs.OnCompletedMux((0,)),
        rs.OnNextMux((1,), 4),
        rs.OnCompletedMux((1,)),
    ]


def test_scan_mux_reduce_empty_on_complete():
    source = [
        rs.OnCreateMux((0,)),
        rs.OnCompletedMux((0,)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed():
        actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.scan(lambda acc, i: i+acc, seed=0, reduce=True),
        )        
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((0,)),
        rs.OnNextMux((0,), 0),
        rs.OnCompletedMux((0,)),
    ]

def test_scan_terminator_no_state():
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed(): actual_completed.append(True)

    rx.from_([1,2,3,4]).pipe(
        rs.ops.scan(
            lambda acc, i: i+acc, seed=0,
            terminator=lambda acc: acc + 1000
        ),
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == [
        1, 3, 6, 10, 1010
    ]


def test_scan_terminator():
    source = [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 2),
        rs.OnNextMux((2,), 2),
        rs.OnNextMux((1,), 1),
        rs.OnCompletedMux((1,)),
        rs.OnCompletedMux((2,)),
    ]
    actual_error = []
    actual_completed = []
    actual_result = []

    def on_completed(): actual_completed.append(True)

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.state.with_memory_store(
            rs.ops.scan(
                lambda acc, i: i+acc, seed=0,
                terminator=lambda acc: 9999
            ),
        )
    ).subscribe(
        on_next=actual_result.append,
        on_completed=on_completed,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_completed == [True]
    actual_result = [r._replace(store=None) for r in actual_result]
    assert actual_result == [
        rs.OnCreateMux((1,)),
        rs.OnNextMux((1,), 1),
        rs.OnCreateMux((2,)),
        rs.OnNextMux((2,), 2),
        rs.OnNextMux((2,), 4),
        rs.OnNextMux((1,), 2),
        rs.OnNextMux((1,), 9999),
        rs.OnCompletedMux((1,)),
        rs.OnNextMux((2,), 9999),
        rs.OnCompletedMux((2,)),
    ]