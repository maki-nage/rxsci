import rx
import rx.operators as ops
from rx.scheduler import ImmediateScheduler


def unframe():
    ''' unframe a Observable of text to lines
    '''
    def _unframe(source):
        def on_subscribe(observer, scheduler):
            acc = ''

            def on_next(i):
                nonlocal acc
                lines = i.split('\n')
                lines[0] = acc + lines[0]
                acc = lines[-1] or ''
                for line in lines[0:-1]:
                    observer.on_next(line)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)

    return _unframe


"""
def unframe():
    ''' unframe a Observable of text to lines
    '''
    def _unframe(source):
        def accumulate(acc, i):
            lines = i.split('\n')
            lines[0] = acc[0] + lines[0]
            return (lines[-1] or '', lines[0:-1])

        return source.pipe(
            ops.scan(accumulate, seed=('', None)),
            ops.filter(lambda i: i[1] is not None),
            ops.flat_map(lambda i: rx.from_(i[1], scheduler=ImmediateScheduler())),
        )

    return _unframe
"""
