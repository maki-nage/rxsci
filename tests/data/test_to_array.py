from array import array
import rx
import rxsci as rs


def test_to_array():
    actual_result = []
    source = [1, 2, 3, 4]

    rx.from_(source).pipe(
        rs.data.to_array('d')
    ).subscribe(
        on_next=actual_result.append
    )

    assert actual_result == [array('d', [1, 2, 3, 4])]
