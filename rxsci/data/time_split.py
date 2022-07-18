from datetime import datetime, timedelta
from typing import Callable, Any
import rx
from rx.subject import Subject
import rxsci as rs
from rxsci.operators.multiplex import demux_mux_observable


def time_split_mux(time_mapper,
    active_timeout, inactive_timeout,
    closing_mapper, include_closing_item):
    outer_observer = Subject()

    def _session_has_expired(start, last, new):
        if active_timeout is not None and new >= start + active_timeout:
            return True
        elif inactive_timeout is not None and new >= last + inactive_timeout:
            return True

        return False

    def _time_split(source):
        def on_subscribe(observer, scheduler):
            state_start = None
            state_last = None

            def on_next(i):
                nonlocal state_start
                nonlocal state_last

                if type(i) is rs.OnNextMux:
                    new_timestamp = time_mapper(i.item)
                    start_timestamp = i.store.get_state(state_start, i.key)
                    last_timestamp = i.store.get_state(state_last, i.key)
                    if start_timestamp is rs.state.markers.STATE_NOTSET:
                        start_timestamp = new_timestamp
                        last_timestamp = new_timestamp
                        i.store.set_state(state_start, i.key, start_timestamp)
                        i.store.set_state(state_last, i.key, last_timestamp)
                        observer.on_next(rs.OnCreateMux((i.key[0], i.key), i.store))

                    if _session_has_expired(start_timestamp, last_timestamp, new_timestamp):
                        i.store.set_state(state_start, i.key, new_timestamp)
                        i.store.set_state(state_last, i.key, new_timestamp)
                        observer.on_next(rs.OnCompletedMux((i.key[0], i.key), i.store))
                        observer.on_next(rs.OnCreateMux((i.key[0], i.key), i.store))
                    elif closing_mapper is not None and closing_mapper(i.item) is True:
                        i.store.set_state(state_start, i.key, new_timestamp)
                        i.store.set_state(state_last, i.key, new_timestamp)
                        if include_closing_item is True:
                            observer.on_next(i._replace(key=(i.key[0], i.key)))
                        observer.on_next(rs.OnCompletedMux((i.key[0], i.key), i.store))
                        observer.on_next(rs.OnCreateMux((i.key[0], i.key), i.store))
                        if include_closing_item is True:
                            return
                    else:
                        i.store.set_state(state_last, i.key, new_timestamp)

                    observer.on_next(i._replace(key=(i.key[0], i.key)))

                elif type(i) is rs.OnCreateMux:
                    i.store.add_key(state_start, i.key)
                    i.store.add_key(state_last, i.key)
                    outer_observer.on_next(i)

                elif type(i) in [rs.OnCompletedMux, rs.OnErrorMux]:
                    start_timestamp = i.store.get_state(state_start, i.key)
                    if start_timestamp is not rs.state.markers.STATE_NOTSET:
                        observer.on_next(i._replace(key=(i.key[0], i.key)))
                    i.store.del_key(state_start, i.key)
                    i.store.del_key(state_last, i.key)
                    outer_observer.on_next(i)

                elif type(i) is rs.state.ProbeStateTopology:
                    state_start = i.topology.create_state(name='time_split', data_type='obj')
                    state_last = i.topology.create_state(name='time_split', data_type='obj')
                    observer.on_next(i)
                    outer_observer.on_next(i)
                else:
                    if state_start is None or state_last is None:
                        observer.on_error(ValueError("No state configured in time split operator. A state store operator is probably missing in the graph"))
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
            )
        return rs.MuxObservable(on_subscribe)

    return _time_split, outer_observer


def time_split(
    time_mapper: Callable[[Any], datetime],
    active_timeout: timedelta=None,
    inactive_timeout: timedelta=None,
    closing_mapper: Callable[[Any], bool]=None,
    include_closing_item: bool=True,
    pipeline=None):
    ''' Splits an observable based on timing criterias.

    Timestamps used to create and expire windows are retrived from the
    time_mapper function.

    The first item of a window is used as a reference to close the window after
    a duration of active_timeout. Each timestamp of the source items is used to
    expire the window after a duration of inactive_timeout without receiving any
    event.

    Additional custom loggic can be implemented with the closing_mapper: In
    addition to the active and inactive timeouts, this mapper can force the
    closing of the current window, and create a new one.

    The source must be a MuxObservable.

    .. marble:: :alt: time_split

        -1-2-3-4-5-6-10---12--|
        [  time_split(5, 3)   ]
        -+---------+-+--------|
                     +10-12---|
                   +6|
         +1-2-3-4-5|

    Args: 
        time_mapper: A function that maps the source items to a datetime object.
        active_timeout: The window expiration duration from the reception date of the first item.
        inactive_timeout: The window expiration duration when no event is received.
        closing_mapper: A function call for each source item and returns whether the window must be closed.
        include_closing_item: whether to include closing items from closing_mapper in the current window (True) or the next window (False).
        pipeline: The Rx pipe to execute on each split.

    Returns: 
        A higher order observable returning on observable for each split window.
    '''
    if pipeline is None:
        raise ValueError('time_split: pipeline cannot be None')

    pipeline = rx.pipe(*pipeline) if type(pipeline) is list else pipeline
    _split, outer_obs = time_split_mux(
        time_mapper=time_mapper,
        active_timeout=active_timeout,
        inactive_timeout=inactive_timeout,
        closing_mapper=closing_mapper,
        include_closing_item=include_closing_item)

    return rx.pipe(
        _split,
        pipeline,
        demux_mux_observable(outer_obs),
    )
