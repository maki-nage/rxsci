import rx


def on_subscribe(post_action):
    def _on_subscribe(source):
        def subscribe(observer, scheduler):
            disposable = source.subscribe(observer, scheduler=scheduler)
            post_action()
            return disposable
        return rx.create(subscribe)

    return _on_subscribe
