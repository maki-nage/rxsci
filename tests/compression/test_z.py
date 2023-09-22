import pytest
from functools import reduce
import rx
import rx.operators as ops
import rxsci.compression.z as z
import rxsci.compression.zstd as zstd


def append():
    def _append(acc, i):
        return acc + i

    return ops.scan(_append, seed=b'')


@pytest.mark.parametrize(
    "compress,decompress",
    [
        pytest.param(
            z.compress, z.decompress,
            id='gzip',
        ),
        pytest.param(
            zstd.compress, zstd.decompress,
            id='zstd',
        )
    ]
)
def test_compress_and_decompress(compress, decompress):
    source = [
        b'The quick brown fox jumps over the lazy dog,The quick brown fox jumps over the lazy dog',
        b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt',
        b'ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco',
        b'laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in ',
        b'voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat ',
        b'non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.',
    ]

    compressed = rx.from_(source).pipe(
        z.compress(),
        append(),
        ops.last(),
    ).run()

    decompressed = rx.just(compressed).pipe(
        z.decompress(),
        append(),
        ops.last(),
    ).run()

    assert len(compressed) < len(decompressed)
    assert decompressed == reduce(lambda acc, i: acc + i, source, b'')
