__author__ = """Romain Picard"""
__email__ = 'romain.picard@oakbits.com'
__version__ = '0.8.0'

from enum import Enum

Padding = Enum('Padding', 'LEFT RIGHT')

from .mux import (
    MuxObservable, cast_as_mux_observable,
    MuxConnectableProxy, cast_as_mux_connectable,
    MuxObserver,
    OnCreateMux, OnNextMux, OnCompletedMux, OnErrorMux, MuxObservable
)

import rxsci.data as data
import rxsci.math as math
import rxsci.operators as ops
