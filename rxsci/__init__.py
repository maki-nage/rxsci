__author__ = """Romain Picard"""
__email__ = 'romain.picard@oakbits.com'
__version__ = '0.6.0'

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

from .assert_ import assert_, assert_1
from .flat_map import flat_map
from .on_subscribe import on_subscribe
from .progress import progress
from .tee_map import tee_map
from .train_test_split import train_test_split
from .with_latest_from import with_latest_from


