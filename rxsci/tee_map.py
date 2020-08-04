import functools
import rx
import rx.operators as ops
from rx.disposable import CompositeDisposable
import rxsci as rs


def _zip(*args, connectable):
    sources = list(args)
    mux = False
    if isinstance(connectable, rs.MuxObservable):
        mux = True

    def subscribe(observer, scheduler):
        n = len(sources)
        queue = [None] * n if mux is False else {}
        has_next = [False] * n
        is_done = [False] * n

        def on_next(i, x):            
            if isinstance(x, rs.OnCreateMux):
                if i == 0:
                    queue[x.key] = [None] * n
                    observer.on_next(x)
                return
            elif isinstance(x, rs.OnCompletedMux) \
                    or isinstance(x, rs.OnErrorMux):                
                if i == n-1:
                    observer.on_next(x)
                    del queue[x.key]
                return

            if mux is True:
                queue[x.key][i] = x.item
            else:
                queue[i] = x
            has_next[i] = True
            if all(has_next):
                try:                    
                    if mux is True:
                        res = tuple(queue[x.key])
                        res = rs.OnNextMux(x.key, res)
                    else:
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
    if mux is False:
        return rx.create(subscribe)
    else:
        return rs.MuxObservable(subscribe)


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
        if isinstance(source, rs.MuxObservable):
            connectable = source.pipe(
                ops.publish(),
                rs.cast_as_mux_connectable(),
            )

        else:
            connectable = source.pipe(
                ops.publish()
            )

        return _zip(
            *[arg(connectable) for arg in args],
            connectable=connectable,
        )

    return _tee_map
