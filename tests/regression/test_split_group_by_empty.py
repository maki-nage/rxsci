import rx
import rxsci as rs


def test_split_group_by_on_empty_source():
    source = []
    actual_result = []
    actual_error = []

    rx.from_(source).pipe(
        rs.state.with_memory_store(pipeline=[
            rs.data.split(predicate=lambda i: i % 1, pipeline=[
                rs.ops.group_by(
                    key_mapper=lambda i: i,
                    pipeline=rx.pipe(
                        rs.data.to_list(),
                    ),
                ),
            ]),
        ])
    ).subscribe(
        on_next=actual_result.append,
        on_error=actual_error.append,
    )

    assert actual_error == []
    assert actual_result == []
