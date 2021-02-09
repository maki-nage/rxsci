import rx
import rxsci.framing.line as line


def unframe(source):
    actual_data = []

    def on_next(i):
        actual_data.append(i)

    rx.from_(source).pipe(
        line.unframe(),
    ).subscribe(
        on_next=on_next,
        on_error=lambda e: print(e)
    )

    return actual_data


def frame(source):
    actual_data = []

    def on_next(i):
        actual_data.append(i)

    rx.from_(source).pipe(
        line.frame(),
    ).subscribe(
        on_next=on_next,
        on_error=lambda e: print(e)
    )

    return actual_data


def test_unframe_complete_lines():
    actual_data = unframe([
        "hello\n",
        "world!\n",
    ])

    assert len(actual_data) == 2
    assert actual_data[0] == 'hello'
    assert actual_data[1] == 'world!'


def test_unframe_partial_lines():
    actual_data = unframe([
        "hell", "o\n",
        "wo", "rld!\n",
    ])

    assert len(actual_data) == 2
    assert actual_data[0] == 'hello'
    assert actual_data[1] == 'world!'


def test_unframe_incomplete_last_line():
    actual_data = unframe([
        "hell", "o\n",
        "wo", "rld!\n",
        "blah",
    ])

    assert len(actual_data) == 3
    assert actual_data[0] == 'hello'
    assert actual_data[1] == 'world!'
    assert actual_data[2] == 'blah'


def test_frame_items():
    actual_data = frame([
        "hello",
        "world!",
    ])

    assert len(actual_data) == 2
    assert actual_data[0] == 'hello\n'
    assert actual_data[1] == 'world!\n'
