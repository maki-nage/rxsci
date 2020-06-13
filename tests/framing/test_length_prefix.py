import rx
import rxsci.framing.length_prefix as lp


def unframe(source):
    actual_data = []

    def on_next(i):
        actual_data.append(i)

    rx.from_(source).pipe(
        lp.unframe(),
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
        lp.frame(),
    ).subscribe(
        on_next=on_next,
        on_error=lambda e: print(e)
    )

    return actual_data


def test_unframe_complete_buffer():
    source1 = b'hello'
    source1 = int(len(source1)).to_bytes(4, byteorder='little') + source1
    source2 = b'world!'
    source2 = int(len(source2)).to_bytes(4, byteorder='little') + source2

    actual_data = unframe([
        source1,
        source2,
    ])

    assert len(actual_data) == 2
    assert actual_data[0] == b'hello'
    assert actual_data[1] == b'world!'


def test_unframe_partial_buffer():
    source1 = b'hello'
    source1 = int(len(source1)).to_bytes(4, byteorder='little') + source1
    source2 = b'world!'
    source2 = int(len(source2)).to_bytes(4, byteorder='little') + source2
    source = source1 + source2

    print(source)

    actual_data = unframe([
        source[0:3], source[3:8],
        source[8:11], source[11:]
    ])

    assert len(actual_data) == 2
    assert actual_data[0] == b'hello'
    assert actual_data[1] == b'world!'


def test_unframe_incomplete_last_item():
    source1 = b'hello'
    source1 = int(len(source1)).to_bytes(4, byteorder='little') + source1
    source2 = b'world!'
    source2 = int(len(source2)).to_bytes(4, byteorder='little') + source2
    source = source1 + source2

    print(source)

    actual_data = unframe([
        source[0:3], source[3:8],
        source[8:11], source[11:-2]
    ])

    assert len(actual_data) == 1
    assert actual_data[0] == b'hello'


def test_frame_items():
    actual_data = frame([
        b'hello',
        b'world!',
    ])

    assert len(actual_data) == 2
    assert actual_data[0] == b'\x05\x00\x00\x00hello'
    assert actual_data[1] == b'\x06\x00\x00\x00world!'
