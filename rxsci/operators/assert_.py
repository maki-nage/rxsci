import rx
import rx.operators as ops
import rxsci as rs


def assert_mux(predicate, name="", error=ValueError):
    '''Ensure that predicate evaluates to True for all items

    If any of the items on the source observable evaluates to False, then
    error is emitted on the on_error handler.

    An optional accumulator function can be provided. If present, all items go
    through the accumulator, and the predicate is called with the result of
    the accumulator. This allows to assert on conditions that span on
    successive items.

    Args:
        predicate: A function to evaluate each item.
        name: [Optional] A firendly name to display with the error.
        error: [Optional] The error to emit when predicate evaluates to False.

    Returns:
        An observable returning the source items, and completing on error if
        any source item evaluates to False.
    '''
    def _assert_mux(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is rs.OnNextMux:
                    try:
                        if predicate(i.item) is True:
                            observer.on_next(rs.OnNextMux(i.key, i.item))
                        else:
                            observer.on_error(error("assert {} failed on: {}".format(name, i.item)))

                    except Exception as e:
                        observer.on_next(rs.OnErrorMux(i.key, e))
                else:
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _assert_mux


def assert_(predicate, name="", error=ValueError):
    '''Ensure that predicate evaluates to True for all items

    If any of item on the source observable evaluates to False, then
    error is emitted on the on_error handler.

    Args:
        predicate: A function to evaluate each item.
        name: [Optional] A firendly name to display with the error.
        error: [Optional] The error to emit when predicate evaluates to False.

    Returns:
        An observable returning the source items, and completing on error if
        any source pair evaluates to False.
    '''
    def _assert_obs(i):
        if predicate(i) is True:
            return i

        raise error("assert {} failed on: {}".format(name, i))

    def _assert(source):
        if isinstance(source, rs.MuxObservable):
            return assert_mux(predicate, name, error)(source)
        else:
            return ops.map(_assert_obs)(source)

    return _assert


def assert_1(predicate, name="", error=ValueError):
    '''Ensures that predicate evaluates to True for all pairs of item / previous item

    If any of the lag1 pair on the source observable evaluates to False, then
    error is emitted on the on_error handler.

    Args:
        predicate: A function to evaluate each item.
        name: [Optional] A firendly name to display with the error.
        error: [Optional] The error to emit when predicate evaluates to False.

    Returns:
        An observable returning the source items, and completing on error if
        any source pair evaluates to False.
    '''
    def _assert_1(source):
        def on_subscribe(observer, scheduler):
            last = None

            def on_next(i):
                nonlocal last
                if last is not None:
                    print(i)
                    if predicate(last, i) is True:
                        observer.on_next(i)
                    else:
                        observer.on_error(error("assert {} failed on: {}-{}".format(name, last, i)))
                else:
                    observer.on_next(i)

                last = i

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler)

        def on_subscribe_mux(observer, scheduler):
            last = {}

            def on_next(i):
                if isinstance(i, rs.OnNextMux):
                    _last = last[i.key]
                    if _last is not None:
                        if predicate(_last, i.item) is True:
                            observer.on_next(rs.OnNextMux(i.key, i))
                        else:
                            observer.on_error(error("assert {} failed on: {}-{}".format(name, last, i.item)))
                    else:
                        observer.on_next(rs.OnNextMux(i.key, i))

                    last[i.key] = i.item

                elif isinstance(i, rs.OnCreateMux):
                    last[i.key] = None
                    observer.on_next(i)
                elif isinstance(i, rs.OnCompletedMux) \
                or isinstance(i, rs.OnErrorMux):
                    del last[i.key]
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler)

        if isinstance(source, rs.MuxObservable):
            return rx.create(on_subscribe_mux)
        else:
            return rx.create(on_subscribe)

    return _assert_1


"""
def assert_1(predicate, name="", error=ValueError):
    '''Ensure that predicate evaluates to True for all pairs of item / previous item

    If any of the lag1 pair on the source observable evaluates to False, then
    error is emitted on the on_error handler.

    Args:
        predicate: A function to evaluate each item.
        name: [Optional] A firendly name to display with the error.
        error: [Optional] The error to emit when predicate evaluates to False.

    Returns:
        An observable returning the source items, and completing on error if
        any source pair evaluates to False.
    '''
    def _assert_mux(mapper):
        def __assert(source):
            def on_subscribe(observer, scheduler):
                last = None
                def on_next(i):
                    if type(i) is rs.OnNextMux:
                        try:
                            ii = mapper(i.item)
                            observer.on_next(rs.OnNextMux(i.key, ii))
                        except Exception as e:
                            observer.on_next(rs.OnErrorMux(i.key, e))
                    elif type(i) is rs.OnCompletedMux:
                    else:
                        observer.on_next(i)
                )

                return source.subscribe(
                    on_next=on_next,
                    on_completed=observer.on_completed,
                    on_error=observer.on_error,
                    scheduler=scheduler
                )

            return rs.MuxObservable(on_subscribe)
        return __assert


    def _assert(i):
        print(i)
        if predicate(i[0], i[1]) is True:
            return i[0]
        
        raise error("assert {} failed on: {}-{}".format(name, i[0], i[1]))

    return rx.pipe(
        rs.data.lag1(),
        rs.ops.map(_assert),
    )
"""
