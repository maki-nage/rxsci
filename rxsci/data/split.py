import rx
from rx.subject import Subject
import rxsci as rs
from rxsci.operators.multiplex import demux_mux_observable


def split_mux(predicate):
    outer_observer = Subject()

    def _split(source):
        def on_subscribe(observer, scheduler):
            state = None

            def on_next(i):
                nonlocal state

                if type(i) is rs.OnNextMux:
                    new_predicate = predicate(i.item)
                    current_predicate = i.store.get_state(state, i.key)
                    if current_predicate is rs.state.markers.STATE_NOTSET:
                        current_predicate = new_predicate
                        i.store.set_state(state, i.key, current_predicate)
                        observer.on_next(rs.OnCreateMux((i.key[0], i.key), i.store))

                    if new_predicate != current_predicate:
                        i.store.set_state(state, i.key, new_predicate)
                        observer.on_next(rs.OnCompletedMux((i.key[0], i.key), i.store))
                        observer.on_next(rs.OnCreateMux((i.key[0], i.key), i.store))

                    observer.on_next(i._replace(key=(i.key[0], i.key)))

                elif type(i) is rs.OnCreateMux:
                    i.store.add_key(state, i.key)
                    outer_observer.on_next(i)

                elif isinstance(i, rs.OnCompletedMux):
                    current_predicate = i.store.get_state(state, i.key)
                    if current_predicate is not rs.state.markers.STATE_NOTSET:
                        observer.on_next(i._replace(key=(i.key[0], i.key)))
                    outer_observer.on_next(i)

                elif type(i) is rs.OnErrorMux:
                    current_predicate = i.store.get_state(state, i.key)
                    if current_predicate is not rs.state.markers.STATE_NOTSET:
                        observer.on_next(i._replace(key=(i.key[0], i.key)))
                    outer_observer.on_next(i)

                elif type(i) is rs.state.ProbeStateTopology:
                    state = i.topology.create_state(name='split', data_type='obj')
                    observer.on_next(i)
                    outer_observer.on_next(i)
                else:
                    if state is None:
                        observer.on_error(ValueError("No state configured in split operator. A state store operator is probably missing in the graph"))
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
            )
        return rs.MuxObservable(on_subscribe)

    return _split, outer_observer


def split(predicate, pipeline):
    ''' Split an observable based on a predicate criteria.

    The source must be a MuxObservable.

    .. marble::
        :alt: split

        -1,a--1,b-1,c-2,b-2,c-|
        [       split()       ]
        -+------------+-------|
                      +2,b-2,c|
         +1,a-1,b--1,c|

    Args:
        predicate: A function called for each item, that returns the split
            criteria.
        pipeline: The Rx pipe to execute on each split.

    Returns:
        A higher order observable returning on observable for each split
        criteria.
    '''
    _split, outer_obs = split_mux(predicate)
    pipeline = rx.pipe(*pipeline) if type(pipeline) is list else pipeline

    return rx.pipe(
        _split,
        pipeline,
        demux_mux_observable(outer_obs),
    )
