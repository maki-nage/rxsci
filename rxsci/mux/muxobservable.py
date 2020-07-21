from rx.core.observable import Observable


class MuxObservable(Observable):
    '''An Observable working on multiplexed observables

    A Muxed observable is an alternative implementation of higher order
    observables. It is specifically designed to work with many observables
    simultaneously with minimal memory and cpu overhead.

    The main difference between higher order observables and muxed observables
    is the fact that subscription and disposal is implicit on muxed observables:
    An operator that subscribes to a muxed observable automatically
    subscribes to all its multiplexed observable on creation and disposes them on
    completion.

    Multuplixed observables must be used with multiplexed observers.
    '''
    def __init__(self, subscribe):
        super().__init__(subscribe)    
