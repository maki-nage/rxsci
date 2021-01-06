from collections import namedtuple
import tempfile
import pytest
import rx
import rx.operators as ops
import rxsci.container.csv as csv


def process(source, pipeline=None):
    actual_data = []
    actual_error = []

    if pipeline is not None:
        source.pipe(*pipeline).subscribe(
            on_next=actual_data.append,
            on_error=actual_error.append,
        )
    else:
        source.subscribe(
            on_next=actual_data.append,
            on_error=actual_error.append,
        )

    if len(actual_error) > 0:
        raise actual_error[0]
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


def test_parser_empty_numbers():
    parser = csv.create_line_parser(
        dtype=[
            ("foo", "int"),
            ("bar", "float"),
        ]
    )

    actual_data = process(rx.from_([
        ",",
    ]), [ops.map(parser)])

    assert len(actual_data) == 1
    assert actual_data[0] == (None, None)


def test_parser_invalid_numbers():
    parser = csv.create_line_parser(
        dtype=[
            ("foo", "int"),
            ("bar", "float"),
        ]
    )

    with pytest.raises(ValueError):
        process(rx.from_([
            "as,ds",
        ]), [ops.map(parser)])


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
        "08,,False",
    ]), [csv.load(parser)])

    assert len(actual_data) == 3
    assert actual_data[0] == (42, 'the', True)
    assert actual_data[1] == (7, 'quick', False)
    assert actual_data[2] == (8, '', False)


def test_load_quoted():
    parser = csv.create_line_parser(
        dtype=[
            ("foo", "int"),
            ("bar", "str"),
        ]
    )

    actual_data = process(rx.from_([
        '1,"the, quick"',
        '2,"\\"brown fox\\""',
        '3,"a\"$#ܟ<a;.b^F ^M^E^Aa^Bov^D^\"[^BƆm^A^Q^]#lx"',
        '4,""',
        '5,"\\"a\\",b"',
        '6,",ab"',
        '7,","',
    ]), [csv.load(parser)])

    assert len(actual_data) == 7
    assert actual_data[0] == (1, 'the, quick')
    assert actual_data[1] == (2, '"brown fox"')
    assert actual_data[2] == (3, 'a"$#ܟ<a;.b^F ^M^E^Aa^Bov^D^"[^BƆm^A^Q^]#lx')
    assert actual_data[3] == (4, '')
    assert actual_data[4] == (5, '"a",b')
    assert actual_data[5] == (6, ',ab')
    assert actual_data[6] == (7, ',')


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
        "a,the,True",
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
        x(foo='cl', bar=None, buz=False),
        x(foo='cl', bar=[1, '2', "3"], buz=False),
    ]
    expected_data = [
        'foo,bar,buz\n',
        '"a",1,True\n',
        '"b",2,False\n',
        '"ab",42,False\n',
        '"cl",,False\n',
        '"cl","[1, \'2\', \'3\']",False\n',
    ]

    actual_data = []
    rx.from_(source).pipe(
        csv.dump(),
    ).subscribe(
        on_next=actual_data.append
    )

    assert actual_data == expected_data


def test_dump_with_quote():
    x = namedtuple('x', ['foo', 'bar', 'buz'])
    source = [
        x(foo='a "is" good', bar=1, buz=True),
        x(foo='"b"', bar=2, buz=False),
        x(foo='a "b"', bar=42, buz=False),
    ]
    expected_data = [
        'foo,bar,buz\n',
        '"a \\"is\\" good",1,True\n',
        '"\\"b\\"",2,False\n',
        '"a \\"b\\"",42,False\n',
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
        'foo,bar,buz\r\n',
        '"a",1,True\r\n',
        '"b",2,False\r\n',
        '"ab",42,False\r\n',
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
        '"a",1,True\n',
        '"b",2,False\n',
        '"ab",42,False\n',
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
        'foo|bar|buz\n',
        '"a"|1|True\n',
        '"b"|2|False\n',
        '"ab"|42|False\n',
    ]

    actual_data = []
    rx.from_(source).pipe(
        csv.dump(separator='|'),
    ).subscribe(
        on_next=actual_data.append
    )

    assert actual_data == expected_data
