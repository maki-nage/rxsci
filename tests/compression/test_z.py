import rx
import rxsci.compression.z as z


def test_compress_and_decompress():
    source = b'The quick brown fox jumps over the lazy dog,The quick brown fox jumps over the lazy dog'

    compressed = rx.just(source).pipe(
        z.compress(),
    ).run()

    decompressed = rx.just(compressed).pipe(
        z.decompress(),
    ).run()

    assert len(compressed) < len(decompressed)
    assert decompressed == source
