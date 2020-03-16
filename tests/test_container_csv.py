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