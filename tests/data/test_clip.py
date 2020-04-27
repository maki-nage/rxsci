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
