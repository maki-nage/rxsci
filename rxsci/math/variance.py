import rx


def variance(key_mapper=lambda i: i, reduce=False):
    '''Computes the variance of the items emitted in the source observable.

    This is an approximation of the real variance. The implementation is based
    on the following article: `standard_deviation <https://www.johndcook.com/blog/standard_deviation/>`_

    Use this operator instead of exact_variance when there are more than 1000
    items in the distribution.

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the variance.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting the variance of the source items. If the number
        of items emitted in the source observable is less than 2, and reduce is set,
        then emits None on completion.
    '''
    def _variance(source):
        def on_subscribe(observer, scheduler):
            m = None
            s = 0
            k = 0

            def on_next(i):
                nonlocal m
                nonlocal s
                nonlocal k
                i = key_mapper(i)

                k += 1
                if m is None:
                    m = i
                else:
                    m1 = m
                    m = m + (i - m) / k
                    s = s + (i - m1)*(i - m)

                if reduce is False and k > 1:
                    observer.on_next(s / (k-1))

            def on_completed():
                if reduce is True:
                    if k < 2:
                        observer.on_next(None)
                        return
                    observer.on_next(s / (k-1))

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)

    return _variance
