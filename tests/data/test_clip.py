import pytest
import rx
import rxsci as rs


def test_clip():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    expected_result = [3, 3, 3, 4, 5, 6, 7, 8, 8, 8]
    actual_result = []

    rx.from_(source).pipe(
        rs.data.clip(lower_bound=3, higher_bound=8)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_clip_lower_bound():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    expected_result = [3, 3, 3, 4, 5, 6, 7, 8, 9, 10]
    actual_result = []

    rx.from_(source).pipe(
        rs.data.clip(lower_bound=3)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_clip_higher_bound():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    expected_result = [1, 2, 3, 4, 5, 6, 7, 8, 8, 8]
    actual_result = []

    rx.from_(source).pipe(
        rs.data.clip(higher_bound=8)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_clip_no_bound():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    actual_result = []

    rx.from_(source).pipe(
        rs.data.clip()
    ).subscribe(on_next=actual_result.append)

    assert actual_result == source


def test_clip_invalid_bound():
    source = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    with pytest.raises(ValueError):
        rx.from_(source).pipe(
            rs.data.clip(lower_bound=8, higher_bound=3)
        ).subscribe()


def test_clip_mux():
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

    rx.from_(source).pipe(
        rs.cast_as_mux_observable(),
        rs.data.clip(lower_bound=2, higher_bound=5)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == [
        rs.OnCreateMux((1 ,None)),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 2),
        rs.OnNextMux((1, None), 3),
        rs.OnNextMux((1, None), 4),
        rs.OnNextMux((1, None), 5),
        rs.OnNextMux((1, None), 5),
        rs.OnCompletedMux((1, None)),
    ]
