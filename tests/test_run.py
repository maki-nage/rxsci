import rx
import rx.operators as ops
import rxsci as rs


def test_run_base():
    source = [1, 2, 3, 4]
    expected_result = [2, 4, 6, 8]

    pipe = rx.from_(source).pipe(
        ops.map(lambda i: i*2),
        ops.to_list(),
    )

    actual_result = rs.run(pipe)

    assert actual_result == expected_result



def test_empty():
    pipe = rx.empty()

    actual_result = rs.run(pipe)

    assert actual_result == None
