=======================
|makinage-logo| RxSci
=======================

.. |makinage-logo| image:: https://github.com/maki-nage/makinage/raw/master/asset/makinage_logo.png

.. image:: https://badge.fury.io/py/rxsci.svg
    :target: https://badge.fury.io/py/rxsci

.. image:: https://github.com/maki-nage/rxsci/workflows/Python%20package/badge.svg
    :target: https://github.com/maki-nage/rxsci/actions?query=workflow%3A%22Python+package%22
    :alt: Github WorkFlows

.. image:: https://github.com/maki-nage/rxsci/raw/master/asset/apis_read.svg
    :target: https://www.makinage.org/doc/rxsci/latest/index.html
    :alt: Documentation


ReactiveX operators for data science and machine learning.

RxSci (pronounced roxy) is a set of RxPY operators and observable factories
dedicated to data science.

Get Started
============

This examples computes a rolling mean on a window and stride of three elements:

.. code:: Python

    import rx
    import rxsci as rs

    source = [1, 2, 3, 4, 5, 6, 7]

    rx.from_(source).pipe(
        rs.ops.multiplex(rx.pipe(
            rs.data.roll(window=3, stride=3, pipeline=rx.pipe(
                rs.math.mean(reduce=True),
            )),
        )),
    ).subscribe(
        on_next=print
    )


See the
`Maki Nage documentation <https://www.makinage.org/doc/makinage-book/latest/index.html>`_
for more information.

Installation
=============

RxSci is available on PyPi and can be installed with pip:

.. code:: console

    pip install rxsci
