from typing import NamedTuple
import os
import tempfile
import pytest

import pyarrow as pa
import pyarrow.parquet as pq
import rx
import rxsci as rs


class NativeStruct(NamedTuple):
    sa: str
    sb: list[int]

class NativeSchema(NamedTuple):
    a: int
    b: str
    c: NativeStruct


schema_struct = pa.struct(fields=[
    ('sa', pa.string()),
    ('sb', pa.list_(pa.uint16())),
])

schema = pa.schema([
    ('a', pa.uint32()),
    ('b', pa.string()),
    ('c', schema_struct),
])


@pytest.mark.parametrize("compression, expected_compression", [
    ("NONE", "UNCOMPRESSED"),
    ("snappy", "SNAPPY"),
    ("zstd", "ZSTD"),
])
def test_dump_to_file(compression, expected_compression):
    source = [
        dict(a=1, b="foo", c=dict(sa="a", sb=[1,2])),
        dict(a=2, b="bar", c=dict(sa="b", sb=[3,4])),
        dict(a=3, b="biz", c=dict(sa="c", sb=[5,6])),
    ]

    with tempfile.TemporaryDirectory() as d:
        f_name = os.path.join(d, "test.parquet")
        rx.from_(source).pipe(
            rs.container.parquet.dump_to_file(
                filename=f_name,
                schema=schema,
                compression=compression,
            )
        ).subscribe()

        table = pq.read_table(f_name)
        metadata = pq.read_metadata(f_name).to_dict()

        assert len(table) == 3
        assert metadata['row_groups'][0]['columns'][0]['compression'] == expected_compression


def test_load_from_file():
    source = [
        dict(a=1, b="foo", c=dict(sa="a", sb=[1,2])),
        dict(a=2, b="bar", c=dict(sa="b", sb=[3,4])),
        dict(a=3, b="biz", c=dict(sa="c", sb=[5,6])),
    ]

    with tempfile.TemporaryDirectory() as d:
        f_name = os.path.join(d, "test.parquet")
        rx.from_(source).pipe(
            rs.container.parquet.dump_to_file(
                filename=f_name,
                schema=schema,
            )
        ).subscribe()

        actual_error = []
        actual_completed = []
        actual_result = []

        def on_completed(): actual_completed.append(True)

        rs.container.parquet.load_from_file(f_name).pipe(
        ).subscribe(
            on_next=actual_result.append,
            on_completed=on_completed,
            on_error=actual_error.append,
        )

        assert actual_error == []
        assert actual_completed == [True]
        assert actual_result == source
