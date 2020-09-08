import rx
from rx.subject import Subject
import rxsci as rs
from rxsci.operators.multiplex import demux_mux_observable


def split_obs(predicate):
    ''' Split an observable based on a predicate criteria.

    Args:
        predicate: A function called for each item, that returns the split 
            criteria.

    Returns:
        A higher order observable returning on observable for each split criteria.
    '''
    def _split(source):
        def on_subscribe(observer, scheduler):
            current_predicate = None
            split_observable = Subject()

            def on_next(i):
                nonlocal current_predicate
                nonlocal split_observable

                new_predicate = predicate(i)
                if current_predicate is None:
                    current_predicate = new_predicate
                    observer.on_next(split_observable)

                if new_predicate != current_predicate:
                    current_predicate = new_predicate
                    split_observable.on_completed()
                    split_observable = Subject()
                    observer.on_next(split_observable)

                split_observable.on_next(i)

            def on_completed():
                split_observable.on_completed()
                observer.on_completed()

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error,
            )
        return rx.create(on_subscribe)
    return _split


def split_mux(predicate):
    outer_observer = Subject()

    def _split(source):
        def on_subscribe(observer, scheduler):
            state = []

            def on_next(i):
                if isinstance(i, rs.OnNextMux):
                    new_predicate = predicate(i.item)
                    current_predicate = state[i.key[0]]
                    if current_predicate is None:
                        current_predicate = new_predicate
                        state[i.key[0]] = current_predicate
                        observer.on_next(rs.OnCreateMux((i.key[0], i.key)))

                    if new_predicate != current_predicate:
                        state[i.key[0]] = new_predicate
                        observer.on_next(rs.OnCompletedMux((i.key[0], i.key)))
                        observer.on_next(rs.OnCreateMux((i.key[0], i.key)))

                    observer.on_next(rs.OnNextMux((i.key[0], i.key), i.item))
                elif isinstance(i, rs.OnCreateMux):
                    append_count = i.key[0] + 1 - len(state)
                    if append_count > 0:
                        for _ in range(append_count):
                            state.append(None)
                    state[i.key[0]] = None
                    outer_observer.on_next(i)
                elif isinstance(i, rs.OnCompletedMux):
                    observer.on_next(rs.OnCompletedMux((i.key[0], i.key)))
                    outer_observer.on_next(i)
                elif isinstance(i, rs.OnErrorMux):
                    observer.on_next(rs.OnErrordMux((i.key[0], i.key), i.error))
                    outer_observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
            )
        return rs.MuxObservable(on_subscribe)

    return _split, outer_observer


def split(predicate, pipeline):
    ''' Split an observable based on a predicate criteria.

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

    Source:
        A MuxObservable

    Returns:
        A higher order observable returning on observable for each split criteria.
    '''
    _split, outer_obs = split_mux(predicate)

    return rx.pipe(
        _split,
        pipeline,
        demux_mux_observable(outer_obs),
    )
