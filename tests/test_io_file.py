import tempfile
import rxsci.io.file as file

def test_read_text():
    with tempfile.NamedTemporaryFile() as f:
        f.write(b'Hello world!')
        f.flush()
        data = file.read(f.name).run()
        assert data == 'Hello world!'


def test_read_binary():
    with tempfile.NamedTemporaryFile() as f:
        f.write(b'Hello world!')
        f.flush()
        data = file.read(f.name, mode='rb').run()
        assert data == b'Hello world!'

def test_read_binary_with_size():
    actual_data = []

    def on_next(i):
        actual_data.append(i)

    with tempfile.NamedTemporaryFile() as f:
        f.write(b'Hello world!')
        f.flush()
        data = file.read(f.name, mode='rb', size=5).subscribe(
            on_next=on_next
        )

        assert len(actual_data) == 3
        assert actual_data[0] == b'Hello'
        assert actual_data[1] == b' worl'
        assert actual_data[2] == b'd!'
