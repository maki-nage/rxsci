.. operators_math:


Math
=====

.. automodule:: rxsci.math
    :members: mean, min, max, sum, variance, stddev

Distributed
------------

This module contains distributed implementations of the math algorithms. They
are useful when distributing computations on several nodes, and reducing then
after.


.. automodule:: rxsci.math.dist
    :members:

Formal
-------

This module contains the exact - formal - implementation of the estimation
algorithms present in the rxsci.math module. Use them with caution because their
precision usually come with either a significant impact on performances, either
with usage restrictions.


.. automodule:: rxsci.math.formal
    :members: variance, stddev
