import rx
import rx.operators as ops
from rx.scheduler import ImmediateScheduler


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
