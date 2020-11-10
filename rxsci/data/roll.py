from collections import deque
from array import array

import rx
import rxsci as rs
from rx.subject import Subject
from rxsci.operators.multiplex import demux_mux_observable


def roll_mux(window, stride):
    outer_observer = Subject()

    density = window // stride
    if window % stride:
        density += 1

    def _roll(source):
        def subscribe(observer, scheduler=None):
            state_n = None
            state_w = None

            def on_next(i):
                nonlocal state_n
                nonlocal state_w
                if isinstance(i, rs.OnNextMux):
                    n = i.store.get_state(state_n, i.key)

                    if (n % stride) == 0:
                        offset = (n // stride) % density
                        index = i.key[0] * density + offset
                        i.store.set_state(state_w, (index, i.key), n)
                        observer.on_next(rs.OnCreateMux((index, i.key), i.store))

                    for offset in range(density):
                        index = i.key[0] * density + offset
                        w_value = i.store.get_state(state_w, (index, i.key))
                        if w_value != -1:
                            observer.on_next(i._replace(key=(index, i.key)))                            
                            count = n - w_value + 1
                            if count == window:
                                i.store.set_state(state_w, (index, i.key), -1)
                                observer.on_next(rs.OnCompletedMux((index, i.key), i.store))

                    n_value = i.store.get_state(state_n, i.key)
                    i.store.set_state(state_n, i.key, n_value+1)

                elif isinstance(i, rs.OnCreateMux):
                    i.store.add_key(state_n, (i.key[0], i.key))
                    for offset in range(density):
                        i.store.add_key(state_w, (i.key[0]+offset, i.key))
                    outer_observer.on_next(i)
                elif isinstance(i, rs.OnCompletedMux):                    
                    kindex = i.key[0]
                    i.store.set_state(state_n, (kindex, i.key), 0)
                    for offset in range(density):
                        index = i.key[0] * density + offset
                        if i.store.get_state(state_w, (index, i.key)) != -1:
                            observer.on_next(i._replace(key=(index, i.key)))
                            i.store.set_state(state_w, (index, i.key), -1)
                    outer_observer.on_next(i)
                elif isinstance(i, rs.OnErrorMux):
                    kindex = i.key[0]
                    i.store.set_state(state_n, (kindex, i.key), 0)
                    for offset in range(density):
                        index = i.key[0] * density + offset
                        if i.store.get_state(state_w, (index, i.key)) != -1:
                            observer.on_next(i._replace(key=(index, i.key)))
                            i.store.set_state(state_w, (index, i.key), -1)
                    outer_observer.on_next(i)
                elif type(i) is rs.state.ProbeStateTopology:                                        
                    state_n = i.topology.create_state(name="roll", data_type='uint', default_value=0)
                    state_w = i.topology.create_state(name="roll", data_type=int, default_value=-1)
                else:
                    observer.on_error(TypeError("scan: unknow item type: {}".format(type(i))))


            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler)

        return rs.MuxObservable(subscribe)

    def _roll_count(source):
        def subscribe(observer, scheduler=None):
            state = array('Q')

            def on_next(i):
                if isinstance(i, rs.OnNextMux):
                    count = state[i.key[0]]
                    if count == 0:
                        observer.on_next(rs.OnCreateMux((0, i.key)))

                    count += 1
                    observer.on_next(rs.OnNextMux((0, i.key), i.item))

                    if count == window:
                        state[i.key[0]] = 0
                        observer.on_next(rs.OnCompletedMux((0, i.key)))
                    else:
                        state[i.key[0]] += 1

                elif isinstance(i, rs.OnCreateMux):
                    append_count = (i.key[0]+1) - len(state)
                    if append_count > 0:
                        for _ in range(append_count):
                            state.append(0)

                    outer_observer.on_next(i)
                elif isinstance(i, rs.OnCompletedMux):
                    if state[i.key[0]] > 0:
                        observer.on_next(rs.OnCompletedMux((0, i.key)))
                    del state[i.key[0]]
                    outer_observer.on_next(i)
                elif isinstance(i, rs.OnErrorMux):
                    if state[i.key[0]] > 0:
                        observer.on_next(rs.OnErrorMux((0, i.key), i.error))
                    del state[i.key[0]]
                    outer_observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler)

        return rs.MuxObservable(subscribe)

    if window == stride:
        return _roll_count, outer_observer
    else:
        return _roll, outer_observer


def roll(window, stride, pipeline):
    """Projects each element of an observable sequence into zero or more
    windows which are produced based on element window information.

    .. marble::
        :alt: roll

        -1--2-3-4--5-6-7--8|
        [     roll(3,3)    ]
        -+------+------+---|
                       +7-8|
                +4-5-6|
         +1-2-3|

    Examples:
        >>> rs.data.roll(3),
        >>> rs.data.roll(window=3, step=2),

    Args:
        window: Length of each window.
        stride: Number of elements to step between creation of
            consecutive windows.
        pipeline: The Rx pipe to execute on each window.

    Source:
        A MuxObservable.

    Returns:
        An observable sequence of windows.

    Raises:
        ValueError if window or stride is negative
    """
    if window <= 0:
        raise ValueError()

    if stride <= 0:
        raise ValueError()

    _roll, outer_obs = roll_mux(window, stride)

    return rx.pipe(
        _roll,
        pipeline,
        demux_mux_observable(outer_obs),
    )
