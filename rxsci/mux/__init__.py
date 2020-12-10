from collections import namedtuple


OnCreateMux = namedtuple('OnCreateMux', ['key', 'store'])
OnCreateMux.__new__.__defaults__ = (None,)
OnCreateMux.__doc__ = "Creates a new MuxObservable"
OnCreateMux.key.__doc__ = "The key identifying the MuxObservable"

OnNextMux = namedtuple('OnNextMux', ['key', 'item', 'store'])
OnNextMux.__new__.__defaults__ = (None,)
OnNextMux.__doc__ = "Emission of an item"
OnNextMux.key.__doc__ = "The key identifying the MuxObservable"
OnNextMux.item.__doc__ = "The item"

OnCompletedMux = namedtuple('OnCompletedMux', ['key', 'store'])
OnCompletedMux.__new__.__defaults__ = (None,)
OnCompletedMux.__doc__ = "Completion of a MuxObservable"
OnCompletedMux.key.__doc__ = "The key identifying the MuxObservable"

OnErrorMux = namedtuple('OnErrorMux', ['key', 'error', 'store'])
OnErrorMux.__new__.__defaults__ = (None,)
OnErrorMux.__doc__ = "Error received on the MuxObservable"
OnErrorMux.key.__doc__ = "The key identifying the MuxObservable"
OnErrorMux.error.__doc__ = "The error"

from .muxobservable import MuxObservable, cast_as_mux_observable
from .muxobserver import MuxObserver
from .muxconnectable import MuxConnectableProxy, cast_as_mux_connectable
