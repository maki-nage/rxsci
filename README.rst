=======================
|makinage-logo| RxSci
=======================

.. |makinage-logo| image:: https://github.com/maki-nage/makinage/raw/master/asset/makinage_logo.png

.. image:: https://img.shields.io/pypi/v/rxsci.svg
    :target: https://pypi.org/project/rxsci/
    :alt: PyPI

.. image:: https://github.com/maki-nage/rxsci/workflows/Python%20package/badge.svg
    :target: https://github.com/maki-nage/rxsci/actions?query=workflow%3A%22Python+package%22
    :alt: Github WorkFlows

.. image:: https://coveralls.io/repos/github/maki-nage/rxsci/badge.svg?branch=master
    :target: https://coveralls.io/github/maki-nage/rxsci?branch=master
    :alt: Code Coverage

.. image:: https://github.com/maki-nage/rxsci/raw/master/asset/apis_read.svg
    :target: https://www.makinage.org/doc/rxsci/latest/index.html
    :alt: Documentation


ReactiveX operators for data science and machine learning.

RxSci is a set of RxPY operators and observable factories dedicated to data
science. Being reactive, RxSci is especially suited to work on streaming data
and time series.

However it can also be used on traditional datasets. Such datasets are processed
as bounded streams by RxSci. So it is possible to use RxSci for both streaming
data and file based datasets. This is especially useful when a
machine learning model is trained with a dataset and deployed on streaming data. 

Get Started
============

This example computes a rolling mean on a window and stride of three elements:

.. code:: Python

    import rx
    import rxsci as rs

    source = [1, 2, 3, 4, 5, 6, 7]

    rx.from_(source).pipe(
        rs.state.with_memory_store([
            rs.data.roll(window=3, stride=3, pipeline=[
                rs.math.mean(reduce=True),
            ]),
        ]),
    ).subscribe(
        on_next=print
    )

.. code:: console

    2.0
    5.0


See the
`Maki Nage documentation <https://www.makinage.org/doc/makinage-book/latest/index.html>`_
for more information.

Installation
=============

RxSci is available on PyPi and can be installed with pip:

.. code:: console

    python3 -m pip install rxsci
