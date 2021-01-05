import copy
import rx
import rxsci as rs
import rx.operators as ops


def scan_mux(accumulator, seed, reduce):
    def _scan(source):
        def on_subscribe(observer, scheduler):
            state = None

            def on_next(i):
                nonlocal state

                if type(i) is rs.OnNextMux:
                    try:
                        value = i.store.get_state(state, i.key)
                        if value is rs.state.markers.STATE_NOTSET:
                            value = seed() if callable(seed) else copy.deepcopy(seed)
                        acc = accumulator(value, i.item)
                        i.store.set_state(state, i.key, acc)
                        if reduce is False:
                            observer.on_next(rs.OnNextMux(i.key, acc, i.store))
                    except Exception as e:
                        observer.on_next(rs.OnErrorMux(i.key, e, i.store))
                elif type(i) is rs.OnCreateMux:
                    i.store.add_key(state, i.key)
                    observer.on_next(i)
                elif type(i) is rs.OnCompletedMux:
                    if reduce is True:
                        value = i.store.get_state(state, i.key)
                        if value is rs.state.markers.STATE_NOTSET:
                            value = seed() if callable(seed) else copy.deepcopy(seed)
                        observer.on_next(rs.OnNextMux(i.key, value, i.store))
                    observer.on_next(i)
                    i.store.del_key(state, i.key)
                elif type(i) is rs.OnErrorMux:                    
                    observer.on_next(i)
                    i.store.del_key(state, i.key)
                elif type(i) is rs.state.ProbeStateTopology:
                    state = i.topology.create_state(name='scan', data_type=type(seed))
                    observer.on_next(i)
                else:
                    observer.on_next(i)

            def on_completed():
                #if reduce is True:
                #    for key, value, is_set in i.store.iterate_state(state):
                #        if not is_set:
                #            value = seed() if callable(seed) else copy.deepcopy(seed)
                #        observer.on_next(rs.OnNextMux(key, value))
                #        observer.on_next(rs.OnCompletedMux(key))
                #state.clear()
                observer.on_completed()

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _scan


def scan(accumulator, seed, reduce=False):
    """Computes an accumulate value on each item of the source observable.

    Applies an accumulator function over an observable sequence and
    returns each intermediate result.

    .. marble::
        :alt: scan

        -1--2--3--4--5--6--|
        [    scan(acc+i)   ]
        -1--3--6--10-15-21-|

    Examples:
        >>> rs.ops.scan(lambda acc, i: acc + i, seed=0, reduce=False)

    Args:
        accumulator: A function called on each item, that accumulates
            tranformation results.
        seed: The initial value of the accumulator. On MuxObservables, seed is
            deep copied for each observable, or called if seed is callable.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An observable sequence containing the accumulated values.
    """

    def _scan(source):
        if isinstance(source, rs.MuxObservable):
            return scan_mux(accumulator, seed, reduce)(source)
        else:
            _seed = seed() if callable(seed) else seed
            if reduce is False:
                return rx.pipe(
                    ops.scan(accumulator, _seed),
                    ops.default_if_empty(default_value=_seed),
                )(source)
            else:
                return rx.pipe(
                    ops.scan(accumulator, _seed),
                    ops.last_or_default(default_value=_seed),
                )(source)

    return _scan
