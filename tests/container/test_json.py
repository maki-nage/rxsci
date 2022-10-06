import os
import json
import tempfile
import pytest
import rx
import rx.operators as ops
import rxsci as rs


def test_load():
    actual_data = rx.from_([
        '{"foo": 4, "bar": "the"}',
        '{"foo": 7, "bar": "quick"}',
        '{"foo": 8}',
    ]).pipe(
        rs.container.json.load(),
        ops.to_list()
    ).run()

    assert actual_data == [
        dict(foo=4, bar="the"),
        dict(foo=7, bar="quick"),
        dict(foo=8),
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
    ]
    expected_data = [
        '{"foo": 4, "bar": "the"}\n',
        '{"foo": 7, "bar": "quick"}\n',
        '{"foo": 8}\n',
    ]

    actual_data = []
    rx.from_(source).pipe(
        rs.container.json.dump(),
    ).subscribe(
        on_next=actual_data.append
    )

    assert actual_data == expected_data


def test_dump_to_file():
    source = [
        dict(foo=4, bar="the"),
        dict(foo=7, bar="quick"),
        dict(foo=8),
    ]
    expected_data = \
        '{"foo": 4, "bar": "the"}\n' \
        '{"foo": 7, "bar": "quick"}\n' \
        '{"foo": 8}\n'

    with tempfile.TemporaryDirectory() as d:
        f = os.path.join(d, "test.csv")
        rx.from_(source).pipe(
            rs.container.json.dump_to_file(f, encoding='utf-8'),
        ).subscribe()

        with open(f, 'r') as f1:
            actual_data = f1.read()
    assert actual_data == expected_data
