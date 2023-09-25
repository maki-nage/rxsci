import io
import rx
import rx.operators as ops
from rx.scheduler import ImmediateScheduler


def frame(prefix_size=4, byteorder='little'):
    ''' Frames an Observable of byte items to a len prefixed streaming format

    The source must be an Observable.
    '''
    mtu = 2**(prefix_size*8)

    def _frame(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if len(i) > mtu:
                    observer.on_error(ValueError("length_prefix: data is too big"))
                data = int(len(i)).to_bytes(prefix_size, byteorder=byteorder)
                data += i
                observer.on_next(data)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)

    return _frame


def unframe(prefix_size=4, byteorder='little'):
    ''' unframe a Observable of bytes from a len prefixed streaming format

    The source must be an Observable.
    '''

    def _unframe(source):
        def on_subscribe(observer, scheduler):
            acc = b''

            def on_next(i):
                nonlocal acc
                offset = 0
                bio = io.BytesIO()
                bio.write(acc)
                bio.write(i)

                bio_len = len(bio.getbuffer())
                bio.seek(offset, io.SEEK_SET)
                while bio_len - offset >= prefix_size:
                    size = int.from_bytes(bio.read(prefix_size), byteorder=byteorder)
                    if bio_len - offset - prefix_size >= size:
                        data = bio.read(size)
                        offset += size + prefix_size
                        observer.on_next(data)
                    else:
                        break

                bio.seek(offset, io.SEEK_SET)
                acc = bio.read()
                bio.close()

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)

    return _unframe
