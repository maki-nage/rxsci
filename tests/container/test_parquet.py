from typing import NamedTuple
import os
import tempfile
import pytest

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError:
     pytest.skip("skipping parquet test without pyarrow installed", allow_module_level=True)

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
    pa.field('sa', pa.string(), nullable=False),
    ('sb', pa.list_(pa.uint16())),
])

schema = pa.schema([
    ('a', pa.uint32()),
    ('b', pa.string()),
    ('c', schema_struct),
])


@pytest.mark.parametrize("sa_null", [True, False])
@pytest.mark.parametrize("open_obj", [True, False])
@pytest.mark.parametrize("compression, expected_compression", [
    ("NONE", "UNCOMPRESSED"),
    ("snappy", "SNAPPY"),
    ("zstd", "ZSTD"),
])
def test_dump_to_file(compression, expected_compression, open_obj, sa_null):
    source = [
        dict(a=1, b="foo", c=dict(sa="a", sb=[1,2])) if sa_null is False else
        dict(a=1, b="foo", c=dict(sb=[1,2])),
        # b is optional on schema, omit on this utterrance to check that
        dict(a=2, c=dict(sa="b", sb=[3,4])),
        dict(a=3, b="biz", c=dict(sa="c", sb=[5,6])),
    ]

    opened = False
    def my_open(f, mode):
        nonlocal opened
        opened = True
        return open(f, mode)

    with tempfile.TemporaryDirectory() as d:
        f_name = os.path.join(d, "test.parquet")
        p = rx.from_(source).pipe(
            rs.container.parquet.dump_to_file(
                filename=f_name,
                schema=schema,
                compression=compression,
                open_obj=my_open if open_obj else open
            )
        )

        if sa_null is True:
            with pytest.raises(pa.lib.ArrowInvalid):
                p.subscribe()
            return

        p.subscribe()
        table = pq.read_table(f_name)
        metadata = pq.read_metadata(f_name).to_dict()

    if open_obj:
        assert opened == True

    assert len(table) == 3
    assert metadata['row_groups'][0]['columns'][0]['compression'] == expected_compression


@pytest.mark.parametrize("open_obj", [True, False])
def test_load_from_file(open_obj):
    source = [
        dict(a=1, b="foo", c=dict(sa="a", sb=[1,2])),
        dict(a=2, c=dict(sa="b", sb=[3,4])),
        dict(a=3, b="biz", c=dict(sa="c", sb=[5,6])),
    ]

    expected_result = [
        dict(a=1, b="foo", c=dict(sa="a", sb=[1,2])),
        dict(a=2, b=None, c=dict(sa="b", sb=[3,4])),
        dict(a=3, b="biz", c=dict(sa="c", sb=[5,6])),
    ]

    opened = False
    def my_open(f, mode):
        nonlocal opened
        opened = True
        return open(f, mode)

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

        rs.container.parquet.load_from_file(
            f_name,
            open_obj=my_open if open_obj else open,
        ).pipe(
        ).subscribe(
            on_next=actual_result.append,
            on_completed=on_completed,
            on_error=actual_error.append,
        )

    if open_obj:
        assert opened == True

    assert actual_error == []
    assert actual_completed == [True]
    assert actual_result == expected_result
