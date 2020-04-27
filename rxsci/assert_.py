import rx


def assert_(predicate,
            accumulator=lambda acc, i: i, seed=None,
            name="", error=ValueError):
    '''Ensure that predicate evaluates to True for all items

    If any of the items on the source observable evaluates to False, then
    error is emitted on the on_error handler.

    An optional accumulator function can be provided. If present, all items go
    through the accumulator, and the predicate is called with the result of
    the accumulator. This allows to assert on conditions that span on
    successive items.

    Args:
        predicate: A function to evaluate each item.
        accumulator: [Optional] An accumulator function to call before predicate.
        seed: [Optional] The accumulator seed.
        error: [Optional] The error to emit when predicate evaluates to False.
        name: [Optional] A firendly name to display with the error.

    Returns:
        An observable returning the source items, and completing on error if
        any source item evaluates to False.
    '''
    def _assert_(source):
        def on_subscribe(observer, scheduler):
            acc = seed

            def on_next(i):
                nonlocal acc
                acc = accumulator(acc, i)
                if predicate(acc) is True:
                    observer.on_next(i)
                else:
                    observer.on_error(error("assert {} failed on: {}".format(name, acc)))

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
            )
        return rx.create(on_subscribe)

    return _assert_
