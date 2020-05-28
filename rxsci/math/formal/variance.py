import rx
from rxsci.math.formal import _moment


def variance(key_mapper=lambda i: i, reduce=False):
    ''' Computes the variance of the items emitted in the source observable.

    The implementation is based on the formal definition of the variance.
    This implies that all items are cached in memory to do the computation.
    Use the rxsci.math.variance operator to compute varianc on a large
    observable.

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the variance.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting variance of source items.
    '''
    def _variance(source):
        def on_subscribe(observer, scheduler):
            q = []

            def on_next(i):
                nonlocal q
                i = key_mapper(i)

                q.append(i)
                if reduce is False:
                    mean = _moment(q, 0, 1)
                    v = _moment(q, mean, 2)
                    observer.on_next(v)

            def on_completed():
                if reduce is True:
                    if len(q) == 0:
                        observer.on_next(None)
                        return
                    mean = _moment(q, 0, 1)
                    v = _moment(q, mean, 2)
                    observer.on_next(v)

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)

    return _variance
