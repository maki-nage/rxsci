Get Started
============

RxSci is a set of RxPY operators and observable factories dedicated to data
science. Being reactive, RxSci is especially suited to work on streaming data
and time series.

However it can also be used on traditional datasets. Such datasets are processed
as bounded streams by RxSci. So it is possible to use RxSci for both streaming
data and file based datasets. This is especially useful when a
machine learning model is trained with a dataset and deployed on streaming data. 

This example computes a rolling mean on a window and stride of three elements:

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

.. code:: console

    2.0
    5.0
