from collections import namedtuple
import tempfile
import rx
import rx.operators as ops
import rxsci.container.csv as csv


def process(source, pipeline=None):
    actual_data = []

    def on_next(i):
        actual_data.append(i)

    if pipeline is not None:
        source.pipe(*pipeline).subscribe(
            on_next=on_next,
            on_error=lambda e: print(e)
        )
    else:
        source.subscribe(
            on_next=on_next,
            on_error=lambda e: print(e)
        )

    return actual_data


def test_parser():
    parser = csv.create_line_parser(
        dtype=[
            ("foo", "int"),
            ("bar", "str"),
            ("buzz", "bool"),
        ]
    )

    actual_data = process(rx.from_([
        "42,the,True",
        "07,quick,False",
    ]), [ops.map(parser)])

    assert len(actual_data) == 2
    assert actual_data[0] == (42, 'the', True)
    assert actual_data[1] == (7, 'quick', False)


def test_load():
    parser = csv.create_line_parser(
        dtype=[
            ("foo", "int"),
            ("bar", "str"),
            ("buzz", "bool"),
        ]
    )

    actual_data = process(rx.from_([
        "42,the,True",
        "07,quick,False",
    ]), [csv.load(parser)])

    assert len(actual_data) == 2
    assert actual_data[0] == (42, 'the', True)
    assert actual_data[1] == (7, 'quick', False)


def test_load_error():
    parser = csv.create_line_parser(
        dtype=[
            ("foo", "int"),
            ("bar", "str"),
            ("buzz", "bool"),
        ]
    )

    actual_data = []
    error = None

    def on_error(e):
        nonlocal error
        error = e

    source = rx.from_([
        "42,the,True",
        "07",
    ])
    source.pipe(csv.load(parser)).subscribe(
            on_next=actual_data.append,
            on_error=on_error,
        )

    assert type(error) == ValueError
    assert len(actual_data) == 1
    assert actual_data[0] == (42, 'the', True)


def test_load_ignore_error():
    parser = csv.create_line_parser(
        dtype=[
            ("foo", "int"),
            ("bar", "str"),
            ("buzz", "bool"),
        ],
        ignore_error=True,
    )

    actual_data = []
    error = None

    def on_error(e):
        nonlocal error
        error = e

    source = rx.from_([
        "42,the,True",
        "07",
    ])
    source.pipe(csv.load(parser)).subscribe(
            on_next=actual_data.append,
            on_error=on_error,
        )

    assert error is None
    assert len(actual_data) == 1
    assert actual_data[0] == (42, 'the', True)


def test_load_from_file():
    parser = csv.create_line_parser(
        dtype=[
            ("foo", "int"),
            ("bar", "str"),
            ("buzz", "bool"),
        ]
    )

    with tempfile.NamedTemporaryFile(mode='w') as f:
        f.write("foo,bar,buzz\n")
        f.write("42,the,True\n")
        f.write("07,quick,False\n")
        f.flush()

        actual_data = process(
            csv.load_from_file(f.name, parser),
            None,
        )

    assert len(actual_data) == 2
    assert actual_data[0] == (42, 'the', True)
    assert actual_data[1] == (7, 'quick', False)


def test_dump():
    x = namedtuple('x', ['foo', 'bar', 'buz'])
    source = [
        x(foo='a', bar=1, buz=True),
        x(foo='b', bar=2, buz=False),
        x(foo='ab', bar=42, buz=False),
    ]
    expected_data = [
        "foo,bar,buz\n",
        "a,1,True\n",
        "b,2,False\n",
        "ab,42,False\n",
    ]

    actual_data = []
    rx.from_(source).pipe(
        csv.dump(),
    ).subscribe(
        on_next=actual_data.append
    )

    assert actual_data == expected_data


def test_dump_with_cr():
    x = namedtuple('x', ['foo', 'bar', 'buz'])
    source = [
        x(foo='a', bar=1, buz=True),
        x(foo='b', bar=2, buz=False),
        x(foo='ab', bar=42, buz=False),
    ]
    expected_data = [
        "foo,bar,buz\r\n",
        "a,1,True\r\n",
        "b,2,False\r\n",
        "ab,42,False\r\n",
    ]

    actual_data = []
    rx.from_(source).pipe(
        csv.dump(newline='\r\n'),
    ).subscribe(
        on_next=actual_data.append
    )

    assert actual_data == expected_data


def test_dump_no_header():
    x = namedtuple('x', ['foo', 'bar', 'buz'])
    source = [
        x(foo='a', bar=1, buz=True),
        x(foo='b', bar=2, buz=False),
        x(foo='ab', bar=42, buz=False),
    ]
    expected_data = [
        "a,1,True\n",
        "b,2,False\n",
        "ab,42,False\n",
    ]

    actual_data = []
    rx.from_(source).pipe(
        csv.dump(header=False),
    ).subscribe(
        on_next=actual_data.append
    )

    assert actual_data == expected_data


def test_dump_pipe_separator():
    x = namedtuple('x', ['foo', 'bar', 'buz'])
    source = [
        x(foo='a', bar=1, buz=True),
        x(foo='b', bar=2, buz=False),
        x(foo='ab', bar=42, buz=False),
    ]
    expected_data = [
        "foo|bar|buz\n",
        "a|1|True\n",
        "b|2|False\n",
        "ab|42|False\n",
    ]

    actual_data = []
    rx.from_(source).pipe(
        csv.dump(separator='|'),
    ).subscribe(
        on_next=actual_data.append
    )

    assert actual_data == expected_data
