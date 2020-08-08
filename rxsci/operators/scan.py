import copy
import rx
import rxsci as rs
from rxsci.internal.utils import NotSet
import rx.operators as ops


def scan_mux(accumulator, seed, reduce):
    def _scan(source):
        def on_subscribe(observer, scheduler):
            state = []
            keys = []

            def on_next(i):
                if type(i) is rs.OnNextMux:
                    try:
                        _state = state[i.key[0]]
                        if _state is NotSet:
                            _state = seed() if callable(seed) else copy.deepcopy(seed)
                        acc = accumulator(_state, i.item)
                        state[i.key[0]] = acc
                        if reduce is False:
                            observer.on_next(rs.OnNextMux(i.key, acc))
                    except Exception as e:
                        observer.on_next(rs.OnErrorMux(i.key, e))
                elif type(i) is rs.OnCreateMux:
                    append_count = (i.key[0]+1) - len(state)
                    if append_count > 0:
                        for _ in range(append_count):
                            state.append(None)
                            keys.append(None)
                    state[i.key[0]] = NotSet
                    keys[i.key[0]] = i.key
                    observer.on_next(i)
                elif type(i) is rs.OnCompletedMux:
                    if reduce is True:
                        _state = state[i.key[0]]
                        if _state is NotSet:
                            _state = seed() if callable(seed) else copy.deepcopy(seed)
                        observer.on_next(rs.OnNextMux(i.key, _state))
                    observer.on_next(i)
                    state[i.key[0]] = None
                    keys[i.key[0]] = None
                elif type(i) is rs.OnErrorMux:
                    observer.on_next(rs.OnErrorMux(i.key, i.error))
                    state[i.key[0]] = None
                    keys[i.key[0]] = None
                else:
                    observer.on_next(TypeError("scan: unknow item type: {}".format(type(i))))

            def on_completed():
                if reduce is True:
                    for index in range(len(keys)):
                        _state = state[index]
                        if _state is NotSet:
                            _state = seed() if callable(seed) else copy.deepcopy(seed)
                            observer.on_next(rs.OnNextMux(keys[index], _state))
                            observer.on_next(rs.OnCompletedMux(keys[index]))
                state.clear()
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
        >>> rs.operators.scan(lambda acc, i: acc + i, seed=0, reduce=False)

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
