import os
import rx


def walk(top, recursive=True):
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

        _walk(top)

    return rx.create(on_subscribe)
