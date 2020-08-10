from collections import deque
from array import array

import rx
import rxsci as rs
from rx.subject import Subject
from rxsci.operators.multiplex import demux_mux_observable


def roll_mux(window, stride=None):
    outer_observer = Subject()

    if window <= 0:
        raise ValueError()

    if stride is None:
        stride = window
    if stride <= 0:
        raise ValueError()

    density = window // stride
    if window % stride:
        density += 1

    def _roll(source):
        def subscribe(observer, scheduler=None):
            state_n = array('Q')
            state_w = array('q')

            def on_next(i):
                if isinstance(i, rs.OnNextMux):
                    n = state_n[i.key[0]]

                    if (n % stride) == 0:
                        index = (n // stride) % density
                        index = i.key[0] * density + index
                        state_w[index] = n
                        observer.on_next(rs.OnCreateMux((index, i.key)))

                    for offset in range(density):
                        index = i.key[0] * density + offset
                        if state_w[index] != -1:
                            observer.on_next(rs.OnNextMux((index, i.key), i.item))
                            count = n - state_w[index] + 1
                            if count == window:
                                state_w[index] = -1
                                observer.on_next(rs.OnCompletedMux((index, i.key)))


                    state_n[i.key[0]] += 1

                elif isinstance(i, rs.OnCreateMux):
                    n_append_count = (i.key[0]+1) - len(state_n)
                    w_append_count = (i.key[0]+1) * density - len(state_w)
                    if n_append_count > 0:
                        for _ in range(n_append_count):
                            state_n.append(0)
                    if w_append_count > 0:
                        for _ in range(w_append_count):
                            state_w.append(-1)
                    outer_observer.on_next(i)
                elif isinstance(i, rs.OnCompletedMux):
                    kindex = i.key[0]
                    state_n[kindex] = 0
                    for offset in range(density):                        
                        state_w[kindex+offset] = -1
                    #observer.on_next(rs.OnCompletedMux((1, i.key)))
                    outer_observer.on_next(i)
                elif isinstance(i, rs.OnErrorMux):
                    kindex = i.key[0]
                    state_n[kindex] = 0
                    for offset in range(density):                        
                        state_w[kindex+offset] = -1
                    #observer.on_next(rs.OnErrordMux((1, i.key), i.error))
                    outer_observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler)

        return rs.MuxObservable(subscribe)

    def _roll_count(source):
        def subscribe(observer, scheduler=None):
            state = {}

            def on_next(i):
                if isinstance(i, rs.OnNextMux):
                    count = state[i.key]
                    '''
                    if count is None:
                        count = 1
                        state[i.key] = 1
                        observer.on_next(rs.OnCreateMux((1, i.key)))
                    '''

                    if count == 0:
                        observer.on_next(rs.OnCreateMux((1, i.key)))

                    count += 1                    
                    observer.on_next(rs.OnNextMux((1, i.key), i.item))
        
                    if count == window:
                        state[i.key] = 0
                        observer.on_next(rs.OnCompletedMux((1, i.key)))
                    else:
                        state[i.key] += 1

                elif isinstance(i, rs.OnCreateMux):
                    state[i.key] = 0
                    outer_observer.on_next(i)
                elif isinstance(i, rs.OnCompletedMux):
                    del state[i.key]
                    outer_observer.on_next(i)
                elif isinstance(i, rs.OnErrorMux):
                    del state[i.key]
                    observer.on_next(rs.OnErrordMux((1, i.key), i.error))
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

        -1-2-3-4-5-6-------|
        [      roll(3)     ]
        -+-----+-----------|
               +4-5-6|
         +1-2-3|

    Examples:
        >>> rs.data.roll(3),
        >>> rs.data.roll(window=3, step=2),

    Args:
        window: Length of each window.
        stride: Number of elements to step between creation of
            consecutive windows.

    Returns:
        An observable sequence of windows.

    Raises:
        ValueError if window or step is negative
    """
    _roll, outer_obs = roll_mux(window, stride)

    return rx.pipe(
        _roll,
        pipeline,
        demux_mux_observable(outer_obs),
    )
