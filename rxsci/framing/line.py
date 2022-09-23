import rx
import rx.operators as ops
from rx.scheduler import ImmediateScheduler


def frame():
    ''' Frames a Observable of text item to lines

    The source must be an Observable.
    '''
    def _frame(source):
        def on_subscribe(observer, scheduler):

            def on_next(i):
                line = ''.join([i, '\n'])
                observer.on_next(line)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler,
            )
        return rx.create(on_subscribe)

    return _frame


def unframe():
    ''' Unframes a Observable of text delimited by new-line

    The source must be an Observable.
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

            def on_completed():
                if len(acc) > 0:
                    observer.on_next(acc)
                observer.on_completed()

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error,
                scheduler=scheduler,
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
