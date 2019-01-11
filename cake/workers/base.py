from abc import ABCMeta, abstractmethod


class BaseWorker(metaclass=ABCMeta):
    def __init__(self, **conf):
        assert 'taskdb_conf' in conf, "The TaskDB config must be given!"
        assert 'taskdb_class' in conf, "The TaskDB class must be given!"
        self._taskdb = conf['taskdb_class'](**conf['taskdb_conf'])

        self._conf = {}
        self._conf.update(conf)
        self._set_defaults()

    @abstractmethod
    def _set_defaults(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        return False

    def close(self):
        self._taskdb.close()

    @abstractmethod
    def run(self, state=None, silent=False):
        """run a task db"""
        pass
