__author__ = """Romain Picard"""
__email__ = 'romain.picard@oakbits.com'
__version__ = '0.2.0'

from enum import Enum

Padding = Enum('Padding', 'LEFT RIGHT')

import rxsci.data as data

from .assert_ import assert_
from .flat_map import flat_map
from .with_latest_from import with_latest_from
from .tee_map import tee_map
from .train_test_split import train_test_split
