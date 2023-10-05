import os
import json
import tempfile
import pytest
import rx
from rx.internal import SequenceContainsNoElementsError
import rx.operators as ops
import rxsci as rs


def test_load():
    actual_data = rx.from_([
        '{"foo": 4, "bar": "the"}',
        '{"foo": 7, "bar": "quick"}',
        '{"foo": 8}',
        '{"foo": "ii: \\"a\\""}\n',
    ]).pipe(
        rs.container.json.load(),
        ops.to_list()
    ).run()

    assert actual_data == [
        dict(foo=4, bar="the"),
        dict(foo=7, bar="quick"),
        dict(foo=8),
        dict(foo='ii: "a"'),
    ]


def test_load_list_indented():
    actual_data = rx.just(
        '[\n' \
        '{"foo": 4, "bar": "the"},\n' \
        '{"foo": 7, "bar": "quick"},\n' \
        '{"foo": 8}\n' \
        ']\n'
    ).pipe(
        rs.container.json.load(),
    ).run()

    assert actual_data == [
        dict(foo=4, bar="the"),
        dict(foo=7, bar="quick"),
        dict(foo=8),
    ]

def test_load_error():
    with pytest.raises(json.decoder.JSONDecodeError):
        actual_data = rx.from_([
            '{"foo": 4, "bar": "the}',
            '{"foo": 7, "bar": "quick"}',
            '{"foo": 8}',
        ]).pipe(
            rs.container.json.load(),
            ops.to_list()
        ).run()


def test_load_ignore_error():
    actual_data = rx.from_([
        '{"foo": 4, "bar": "the}',
        '{"foo": 7, "bar": "quick"}',
        '{"foo": 8}',
    ]).pipe(
        rs.container.json.load(ignore_error=True),
        ops.to_list()
    ).run()

    assert actual_data == [
        dict(foo=7, bar="quick"),
        dict(foo=8),
    ]


def test_load_from_file():
    with tempfile.TemporaryDirectory() as d:
        f_name = os.path.join(d, "test.csv")

        with open(f_name, mode="w") as f:
            f.write('{"foo": 4, "bar": "the"}\n')
            f.write('{"foo": 7, "bar": "quick"}\n')
            f.write('{"foo": 8}\n')
            f.flush()

        actual_data = rs.container.json.load_from_file(f_name).pipe(
            ops.to_list()
        ).run()

    assert actual_data == [
        dict(foo=4, bar="the"),
        dict(foo=7, bar="quick"),
        dict(foo=8),
    ]


def test_load_from_file_with_errors():
    with tempfile.TemporaryDirectory() as d:
        f_name = os.path.join(d, "test.csv")

        with open(f_name, mode="w") as f:
            f.write('{"foo": 4, "bar": "the"}\n')
            f.write('{"foo": 7, "bar": "quick"2}\n')
            f.write('{"foo": 8}\n')
            f.flush()

        actual_data = rs.container.json.load_from_file(
            f_name,
            ignore_error=True,
        ).pipe(
            ops.to_list()
        ).run()

    assert actual_data == [
        dict(foo=4, bar="the"),
        dict(foo=8),
    ]


def test_load_from_no_lines():
    with tempfile.TemporaryDirectory() as d:
        f_name = os.path.join(d, "test.csv")

        with open(f_name, mode="w") as f:
            f.write('[\n')
            f.write('{"foo": 4, "bar": "the"},\n')
            f.write('{"foo": 7, "bar": "quick"},\n')
            f.write('{"foo": 8}\n')
            f.write(']\n')
            f.flush()

        actual_data = rs.container.json.load_from_file(f_name, lines=False).run()

    assert actual_data == [
        dict(foo=4, bar="the"),
        dict(foo=7, bar="quick"),
        dict(foo=8),
    ]


def test_dump():
    source = [
        dict(foo=4, bar="the"),
        dict(foo=7, bar="quick"),
        dict(foo=8),
        dict(foo='ii:"a"'),
    ]
    expected_data = [
        '{"foo":4,"bar":"the"}\n',
        '{"foo":7,"bar":"quick"}\n',
        '{"foo":8}\n',
        '{"foo":"ii:\\"a\\""}\n',
    ]

    actual_data = []
    rx.from_(source).pipe(
        rs.container.json.dump(),
    ).subscribe(
        on_next=actual_data.append
    )

    assert [ e.replace(' ', '') for e in actual_data] == expected_data


def test_dump_to_file():
    source = [
        dict(foo=4, bar="the"),
        dict(foo=7, bar="quick"),
        dict(foo=8),
    ]
    expected_data = \
        '{"foo":4,"bar":"the"}\n' \
        '{"foo":7,"bar":"quick"}\n' \
        '{"foo":8}\n'

    with tempfile.TemporaryDirectory() as d:
        f = os.path.join(d, "test.csv")
        rx.from_(source).pipe(
            rs.container.json.dump_to_file(f, encoding='utf-8'),
        ).subscribe()

        with open(f, 'r') as f1:
            actual_data = f1.read()
    assert actual_data.replace(' ', '') == expected_data
    assert actual_data.replace(' ', '') == expected_data


@pytest.mark.parametrize(
    "compression",
    ['gzip', 'zstd'],
)
def test_dump_load_with_compression(compression):
    source = [
        dict(foo=i, bar="the")
        for i in range(100000)
    ]

    with tempfile.TemporaryDirectory() as d:
        f = os.path.join(d, "test.csv")
        try:
            rx.from_(source).pipe(
                rs.container.json.dump_to_file(f, compression=compression),
            ).run()
        except SequenceContainsNoElementsError:
            pass

        actual_data = rs.container.json.load_from_file(f, compression=compression).pipe(
            rs.data.to_list(),
        ).run()

    assert actual_data == source
