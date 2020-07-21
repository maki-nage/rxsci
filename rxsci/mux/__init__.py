from collections import namedtuple


OnCreateMux = namedtuple('OnCreateMux', ['key'])
OnNextMux = namedtuple('OnNextMux', ['key', 'item'])
OnCompletedMux = namedtuple('OnCompletedMux', ['key'])
OnErrorMux = namedtuple('OnErrorMux', ['key', 'error'])

from .muxobservable import MuxObservable
from .muxobserver import MuxObserver
