import random
import rx
import rxsci as rs


def test_sort():
    source = [2, 3, 10, 4, 1, 5, 6, 7, 5, 9, 8, 9, 11, 11]
    expected_result = [1, 2, 3, 4, 5, 5, 6, 7, 8, 9, 9, 10, 11, 11]
    actual_result = []

    rx.from_(source).pipe(
        rs.data.sort()
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_sort_key():
    source = [2, 3, 10, 4, 1, 5, 6, 7, 5, 9, 8, 9, 11, 11]
    expected_result = [11, 11, 10, 9, 9, 8, 7, 6, 5, 5, 4, 3, 2, 1]
    actual_result = []

    rx.from_(source).pipe(
        rs.data.sort(key=lambda i: -i)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result


def test_sort_random():
    random.seed(42)
    source = [random.randint(0, 1337) for _ in range(42424)]
    expected_result = sorted(source, reverse=True)
    actual_result = []

    rx.from_(source).pipe(
        rs.data.sort(key=lambda i: -i)
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result
