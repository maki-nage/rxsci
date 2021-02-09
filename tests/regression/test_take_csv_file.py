import rxsci as rs
import rxsci.container.csv as csv


def test_take_5_lines():
    parser = csv.create_line_parser(
        dtype=[
            ('name', 'str'),
            ('value', 'int'),
        ]
    )

    data = []
    csv.load_from_file("tests/sample.csv", parse_line=parser).pipe(
        rs.ops.take(5),
    ).subscribe(on_next=data.append)

    assert len(data) == 5
    assert data[0] == ('foo', 1)
    assert data[1] == ('bar', 2)
    assert data[2] == ('biz', 3)
    assert data[3] == ('buz', 4)
    assert data[4] == ('fiz', 5)
