import zstandard
import rx
import rx.operators as ops


def compress():
    ''' Compresses an Observable of bytes with zstd-compression

    The incoming elements are compressed in streaming mode until the
    observable completed.

    The source must be an Observable.
    '''

    def _compress(source):
        def on_subscribe(observer, scheduler):
            compressor = zstandard.ZstdCompressor()
            compressor = compressor.compressobj()

            def on_next(i):
                try:
                    data = compressor.compress(i)
                    observer.on_next(data)
                except Exception as e:
                    observer.on_error(e)

            def on_completed():
                try:
                    data = compressor.flush()
                    observer.on_next(data)
                    observer.on_completed()
                except Exception as e:
                    observer.on_error(e)

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)

    return _compress


def decompress():
    ''' Decompresses an Observable of bytes with zstd-compression

    The incoming elements are decompressed in streaming mode until the
    observable completed.

    The source must be an Observable.
    '''
    def _decompress(source):
        def on_subscribe(observer, scheduler):
            decompressor = zstandard.ZstdDecompressor()
            decompressor = decompressor.decompressobj()

            def on_next(i):
                try:
                    data = decompressor.decompress(i)
                    observer.on_next(data)
                except Exception as e:
                    observer.on_error(e)

            def on_completed():
                try:
                    if not decompressor.eof:
                        observer.on_error(RuntimeError("zstd.decompress: Invalid state at observable completion"))
                    else:
                        data = decompressor.flush()
                        observer.on_next(data)
                        observer.on_completed()
                except Exception as e:
                    observer.on_error(e)

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)

    return _decompress
