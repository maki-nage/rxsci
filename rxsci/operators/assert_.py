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
            state = None

            def on_next(i):
                nonlocal state
                if isinstance(i, rs.OnNextMux):
                    value = i.store.get_state(state, i.key)
                    if value is not rs.state.markers.STATE_NOTSET:
                        if predicate(value, i.item) is True:
                            observer.on_next(rs.OnNextMux(i.key, i))
                        else:
                            observer.on_error(error("assert {} failed on: {}-{}".format(name, value, i.item)))
                    else:
                        observer.on_next(rs.OnNextMux(i.key, i))

                    i.store.set_state(state, i.key, i.item)

                elif isinstance(i, rs.OnCreateMux):
                    i.store.add_key(state, i.key)
                    observer.on_next(i)

                elif isinstance(i, rs.OnCompletedMux) \
                or isinstance(i, rs.OnErrorMux):
                    i.store.del_key(state, i.key)
                    observer.on_next(i)

                elif type(i) is rs.state.ProbeStateTopology:
                    state = i.topology.create_state(name='assert1', data_type='obj')
                    observer.on_next(i)

                else:
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
