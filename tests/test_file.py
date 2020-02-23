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
