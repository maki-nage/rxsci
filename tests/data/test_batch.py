import rx
import rxsci as rs


def test_batch():
    source = [1, 2, 3, 4, 5, 6, 7, 8]
    actual_result = []
    expected_result = [
        [1,2,3],
        [4,5,6],
        [7,8],
    ]

    rx.from_(source).pipe(
        rs.state.with_memory_store(
            rs.data.batch(3),
        ),
    ).subscribe(on_next=actual_result.append)

    assert actual_result == expected_result
    