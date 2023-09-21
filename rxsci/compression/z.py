import zlib
import rx
import rx.operators as ops


def compress():
    ''' Compresses an Observable of byte with zlib-compression

    The source must be an Observable.
    '''

    def _frame(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                data = zlib.compress(i)[2:-1]  #  wbits=-15)[2:-1]
                observer.on_next(data)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)

    return _frame


def decompress():
    ''' Decompresses an Observable of bytes with zlib-compression

    The source must be an Observable.
    '''

    def _unframe(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                data = zlib.decompress(i, wbits=-15)
                observer.on_next(data)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)

    return _unframe
