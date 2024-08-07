import os
import tempfile
import rxsci.io.file as file

import rx
import rx.operators as ops


def test_read_text_source():
    with tempfile.TemporaryDirectory() as d:
        f_name = os.path.join(d, "test")
        with open(f_name, mode="wb") as f:
            f.write(b'Hello world!')
            f.flush()

        data = file.read(f_name).run()
        assert data == 'Hello world!'


def test_read_text_custom_open():
    opened = False
    def my_open(f, mode, encoding):
        nonlocal opened
        opened = True
        return open(f, mode, encoding=encoding)

    with tempfile.TemporaryDirectory() as d:
        f_name = os.path.join(d, "test")
        with open(f_name, mode="wb") as f:
            f.write(b'Hello world!')
            f.flush()

        data = file.read(f_name, open_obj=my_open).run()
        assert data == 'Hello world!'
        assert opened == True


def test_read_binary():
    with tempfile.TemporaryDirectory() as d:
        f_name = os.path.join(d, "test")
        with open(f_name, mode="wb") as f:
            f.write(b'Hello world!')
            f.flush()
        data = file.read(f_name, mode='rb').run()
        assert data == b'Hello world!'


def test_read_binary_with_size():
    actual_data = []

    def on_next(i):
        actual_data.append(i)

    with tempfile.TemporaryDirectory() as d:
        f_name = os.path.join(d, "test")
        with open(f_name, mode="wb") as f:
            f.write(b'Hello world!')
            f.flush()

        file.read(f_name, mode='rb', size=5).subscribe(
            on_next=on_next
        )

        assert len(actual_data) == 3
        assert actual_data[0] == b'Hello'
        assert actual_data[1] == b' worl'
        assert actual_data[2] == b'd!'
