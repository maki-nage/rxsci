from array import array
import functools
import rx
import rx.operators as ops
from rx.disposable import CompositeDisposable
import rxsci as rs


def _process_many(*args, connectable, zip):
    sources = list(args)

    def subscribe_mux(observer, scheduler):
        n = len(sources)
        queue = []
        has_next = array('B')

        def on_next(i, x):
            if isinstance(x, rs.OnCreateMux):
                if i == 0:
                    if zip is True:
                        append_count = (x.key[0]+1) * n - len(queue)
                        if append_count > 0:
                            for _ in range(append_count):
                                queue.append(None)
                                has_next.append(False)
                    observer.on_next(x)
                return
            elif isinstance(x, rs.OnCompletedMux):
                if i == n-1:
                    observer.on_next(x)
                return
            elif isinstance(x, rs.OnErrorMux):
                observer.on_next(x)
                return

            if zip is True:
                base_index = x.key[0] * n
                index = base_index + i
                queue[index] = x.item
                has_next[index] = True
                _next = has_next[base_index:base_index+n]
                if all(_next):
                    try:
                        _queue = queue[base_index:base_index+n]
                        res = tuple(_queue)
                        res = rs.OnNextMux(x.key, res)
                        for index in range(n):
                            has_next[base_index+index] = False
                    except Exception as ex:  # pylint: disable=broad-except
                        observer.on_error(ex)
                        return

                    observer.on_next(res)
            else:
                observer.on_next(x)

        subscriptions = [None] * n
        for i in range(n):
            subscriptions[i] = sources[i].subscribe_(
                on_next=functools.partial(on_next, i),
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler,
            )
        subscriptions.append(connectable.connect())
        return CompositeDisposable(subscriptions)

    def subscribe(observer, scheduler):
        n = len(sources)
        queue = [None] * n
        has_next = [False] * n
        is_done = [False] * n

        def on_next(i, x):
            if zip is True:
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
            else:
                observer.on_next(x)

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

    if isinstance(connectable, rs.MuxObservable):
        return rs.MuxObservable(subscribe_mux)
    else:
        return rx.create(subscribe)


def tee_map(*args, zip=True):
    '''Processes several operators chains simultaneously on the same source
    observable. This operator allows to do multiple processing on the same
    source, and combine the results as a single tuple object.

    .. marble::
        :alt: tee_map

        --4---2---1---3-----|
        [tee_map(count, min)]
        --1,4-2,2-3,1-4,1---|

    Examples:
        >>> rx.from_([1, 2, 3, 4]).pipe(
        >>>     rs.ops.tee_map(
        >>>         rs.ops.count(),
        >>>         rs.math.min(),
        >>>))

    Args:
        args: list of operators that will process the source observable
            concurrently
        zip: [Optional] If set to True, then all processing are zipped
            together. Otherwise items are emitted by tee map as they are
            emitted by each arg pipeline.

    Source:
        An Observable or a MuxObservable

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

        return _process_many(
            *[arg(connectable) for arg in args],
            connectable=connectable,
            zip=zip
        )

    return _tee_map
