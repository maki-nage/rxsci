from typing import NamedTuple

import rx
from rx.scheduler import CurrentThreadScheduler
from rx.disposable import CompositeDisposable, Disposable
import rxsci as rs


try:
    import pyarrow as pa
    import pyarrow.parquet as pq


    def create_record(schema: pa.Schema):
        '''Transpose a batch of items into a batch of column-oriented records.
        '''
        columns_name = schema.names
        columns_type = [t for t in schema.types]

        def _create_record(data):
            columns_data = [ [] for n in columns_name]

            for v in data:
                for i, n in enumerate(columns_name):
                    columns_data[i].append(v.get(n, None))

            try:
                record_batch = pa.RecordBatch.from_arrays(
                    [
                        
                        pa.array(columns_data[i], type=columns_type[i])
                        for i, n in enumerate(columns_name)
                    ],
                    schema=schema,
                )
            except Exception as e:
                print(data)
                raise e

            
            return record_batch
        
        return _create_record


    def to_record(schema):
        return rs.ops.map(create_record(schema))


    def _dump_parquet(
        filename: str,
        schema: pa.Schema,
        row_group_size: int = None,
        compression: str = None,
        encryption_properties: pq.FileEncryptionProperties = None,
        open_obj=open,
    ):
        _schema = schema

        def _dump(source):
            def on_subscribe(observer, scheduler):
                try:
                    f = filename
                    if type(filename) is str:
                        f = open_obj(filename, mode='wb')

                    writer = pq.ParquetWriter(
                        f, schema,
                        compression=compression,
                        encryption_properties=encryption_properties,
                        #write_statistics=False,
                    )
                except Exception as e:
                    observer.on_error(e)

                def on_completed():
                    nonlocal writer

                    writer.close()
                    writer = None
                    if type(filename) is str:
                        f.close()

                    observer.on_completed()

                def on_error(e):
                    nonlocal writer

                    writer.close()
                    writer = None

                    if type(filename) is str:
                        f.close()
                    observer.on_error(e)

                def on_next(i):
                    try:
                        writer.write(i, row_group_size=row_group_size)
                    except Exception as e:
                        on_error(e)

                return source.subscribe(
                    on_next=on_next,
                    on_completed=on_completed,
                    on_error=on_error,
                    scheduler=scheduler,
                )
            return rx.create(on_subscribe)

        return _dump


    def load_from_file(
        filename: str,
        batch_size: int = 1024,
        decryption_properties: pq.FileDecryptionProperties = None,
        open_obj=open,
    ):
        '''loads a parquet file.

        This factory loads the provided parquet file.

        The open_obj function must return a file-like object. Its prototype is:
        open_obj(filename: str, mode: str, encoding: str) -> file-like object

        Args:
            filename: Path of the file to write or a file object
            batch_size: Size of internal batches when writing in the parquet file
            decryption_properties: [Optional] decryption configuration.
            open_obj: A function to open the source file.

        Returns:
            An observable of objects.
        '''
        def on_subscribe(observer, scheduler_):
            disposed = False
            _scheduler = scheduler_ or CurrentThreadScheduler.singleton()


            def _load_file(filename):
                pf = pq.parquet_file = pq.ParquetFile(
                    filename,
                    decryption_properties=decryption_properties,
                )

                schema = pf.schema.to_arrow_schema()
                schema_names = schema.names
                for batch in pf.iter_batches(batch_size = batch_size, use_threads=False):
                    rows = [dict(zip(schema_names, row)) for row in zip(*batch.to_pydict().values())]
                    for r in rows:
                        observer.on_next(r)
                    if disposed:
                        break

                pf.close()
                observer.on_completed()

            def _action(_, __):
                try:
                    if type(filename) is str:
                        with open_obj(filename, mode='rb') as f:
                            _load_file(f)
                    else:
                        _load_file(filename)

                except Exception as e:
                    observer.on_error(e)

            def _dispose():
                nonlocal disposed
                disposed = True

            disp = Disposable(_dispose)
            return CompositeDisposable(_scheduler.schedule(_action), disp)

        return rx.create(on_subscribe)


    def dump_to_file(
        filename: str,
        schema: pa.Schema,
        batch_size: int = 1024,
        row_group_size: int = None,
        compression: str = 'snappy',
        encryption_properties: pq.FileEncryptionProperties = None,
        open_obj=open,
    ):
        '''dumps each item to a parquet file.

        The source must be an Observable.

        Args:
            filename: Path of the file to write or a file object
            schema: The schema of the data to write.
            batch_size: Size of internal batches when writing in the parquet file
            row_group_size: [Optional] Size of a row group. See pyarrow.parquet.ParquetWriter for more informaton.
            compression: [Optional] compression codec: none, snappy, gzip, brotli, lz4, zstd.
            encryption_properties: [Optional] encryption configuration.

        Returns:
            An empty observable that completes on success when the source
            observable completes or completes on error if there is an error
            while writing the parquet.
        '''
        def _dump_to_file(source):
            return source.pipe(
                rs.data.batch(batch_size=batch_size),
                to_record(schema),
                _dump_parquet(
                    filename=filename,
                    schema=schema,
                    row_group_size=row_group_size,
                    compression=compression,
                    encryption_properties=encryption_properties,
                    open_obj=open_obj,
                ),
            )
        return _dump_to_file


except Exception:
    def dump_to_file(
        filename,
        schema,
        batch_size=1024,
        row_group_size=None,
        compression='snappy',
        encryption_properties=None,
        open_obj=open,
    ):
        raise ImportError('pyarrow not found. Please install it to use this operator')
    
    def load_from_file(
        filename,
        batch_size=1024,
        decryption_properties=None,
        open_obj=open,
    ):
        raise ImportError('pyarrow not found. Please install it to use this operator')
