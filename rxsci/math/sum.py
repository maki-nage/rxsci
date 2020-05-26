import rx


def sum(key_mapper=lambda i: i, reduce=False):
    '''Sums the items emitted in the source observable

    Args:
        key_mapper: [Optional] a function called on each item before computing
            the sum.
        reduce: [Optional] Emit an item for each source item when reduce is 
            False, otherwise emits a single item on completion.

    Returns:
        An observable emitting items whose value is the sum of source items.
    '''
    def _sum(source):
        def on_subscribe(observer, scheduler):
            s = 0

            def on_next(i):
                nonlocal s
                i = key_mapper(i)

                s += i
                if reduce is False:
                    observer.on_next(s)

            def on_completed():
                if reduce is True:
                    observer.on_next(s)

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error
            )
        return rx.create(on_subscribe)
    return _sum
