from collections import namedtuple


OnCreateMux = namedtuple('OnCreateMux', ['key'])
OnNextMux = namedtuple('OnNextMux', ['key', 'item'])
OnCompletedMux = namedtuple('OnCompletedMux', ['key'])
OnErrorMux = namedtuple('OnErrorMux', ['key', 'error'])

from .muxobservable import MuxObservable, cast_as_mux_observable
from .muxobserver import MuxObserver
from .muxconnectable import MuxConnectableProxy, cast_as_mux_connectable
from .state import MuxState
