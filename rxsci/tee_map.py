import functools
import rx
import rx.operators as ops
from rx.disposable import CompositeDisposable


def _zip(*args, connectable=None):
    sources = list(args)

    def subscribe(observer, scheduler):
        n = len(sources)
        queue = [None] * n
        has_next = [False] * n
        is_done = [False] * n

        def on_next(i, x):
            queue[i] = x
            has_next[i] = True
            if all(has_next):
                try:
                    res = tuple(queue)
                    for index in range(n):
                        has_next[index] = False
                except Exception as ex:  # pylint: disable=broad-except
                    observer.on_error(ex)
                    return

                observer.on_next(res)

        def done(i):
            is_done[i] = True
            if all(is_done):
                observer.on_completed()

        subscriptions = [None] * n
        for i in range(n):
            subscriptions[i] = sources[i].subscribe_(
                functools.partial(on_next, i),
                observer.on_error,
                functools.partial(done, i),
                scheduler
            )
        subscriptions.append(connectable.connect())
        return CompositeDisposable(subscriptions)
    return rx.create(subscribe)


def tee_map(*args):
    '''Processes several operators chains simultaneously on the same source
    observable. This operator allows to do multiple processing on the same
    source, and combine the results as a single tuple object.

    Args:
        args: list of operators that will process the source observable
            concurrently

    Returns:
        An observable containing tuples of the items emitted by each branch of
        the tee.
    '''
    def _tee_map(source):
        connectable = source.pipe(
            ops.publish()
        )

        return _zip(
            *[arg(connectable) for arg in args],
            connectable=connectable,
        )

    return _tee_map
