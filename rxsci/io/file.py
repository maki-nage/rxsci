import rx

def read(file, mode='r', size=None):
    ''' Reads the content of a file

    Args: 
        file: the path of the file to read 
        mode: how the file must be opened. either 'r' to read text or 'rb' to read binary
        size: Optional. If set file if read by chunks of this size

    Returns: 
        An observable where eeach item is a chunk of data, or the while
        file if no size has been set.
    '''
    def on_subscribe(observer, scheduler):
        try:
            with open(file, mode) as f:
                if size is None:
                    data = f.read(size)
                    observer.on_next(data)
                    observer.on_completed()
        except Exception as e:
            observer.on_error(e)

    return rx.create(on_subscribe)