import os
import rx


def walk(top, recursive=True):
    ''' lists the content of a directory

    Returns an observable emitting the files present in top directory and
    sub-directories.

    Args:
        top: The directory to walk
    Returns:
        An Observable emitting string path items
    '''
    def on_subscribe(observer, scheduler):
        def _walk(top):
            for path, dirs, files in os.walk(top):
                for filename in files:
                    observer.on_next(os.path.join(path, filename))
                '''
                if recursive is True:
                    for dirname in dirs:
                        print("bbb: {}".format(dirname))
                        _walk(os.path.join(path, dirname))
                '''
            observer.on_completed()
        _walk(top)

    return rx.create(on_subscribe)
