import pytest
import rxsci as rs


@pytest.mark.parametrize(
    "progress",
    [
        False, True,
        {'interval': 60},
        {'prefix': 'test_from'},
        {'prefix': 'test_from', 'interval': 60}
    ]
)
def test_from_iterable_base(progress):
    actual_result = []
    actual_error = []

    rs.ops.from_iterable(range(100), progress=progress).subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_result == list(range(100))
