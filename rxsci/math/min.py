import rx


def min(key_mapper=lambda i: i, reduce=False):
    '''Returns the minimum value emitted in the source observable.

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the min.
        reduce: [Optional] Emit an item for each source item when reduce is 
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting the min of source items.
    '''
    def _min(source):
        def on_subscribe(observer, scheduler):
            m = None

            def on_next(i):
                nonlocal m
                i = key_mapper(i)

                if m is None or i < m:
                    m = i
                if reduce is False:
                    observer.on_next(m)

            def on_completed():
                if reduce is True or m is None:
                    observer.on_next(m)
                observer.on_completed()

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error,
                scheduler=scheduler,
            )
        return rx.create(on_subscribe)
    return _min
