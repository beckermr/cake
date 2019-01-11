import sys
import signal
import numpy as np
import time
import subprocess
import multiprocessing

from .base import BaseWorker
from ..defaults import TASKDB_STATES, TASK_STATES
from ..utils import print_start, print_end

_task_infos = []
_taskdb = None
_pool = None


def _signal_handler(signal, frame):
    if _pool is not None and len(_task_infos) > 0:
        _pool.terminate()
        _pool.join()

    if _taskdb is not None:
        for _task_info in _task_infos:
            if not _task_info[2]:
                etime = time.time()
                print_end(_task_info[0], _task_info[1], etime, TASK_STATES.KILLED)
            _taskdb.checkin(_task_info[0], TASK_STATES.KILLED)

    sys.exit(0)


def _run_work(tsk, id, silent):
    stime = time.time()
    if not silent:
        print_start(id, stime)

    err = subprocess.run(tsk, shell=True).returncode
    etime = time.time()

    if not silent:
        if err != 0:
            print_end(id, stime, etime, TASK_STATES.FAILED)
        else:
            print_end(id, stime, etime, TASK_STATES.SUCCEEDED)

    return id, err


class PMPWorker(BaseWorker):
    def __init__(self, **conf):
        super().__init__(**conf)
        assert 'n' in conf, "The number pool members must be given!"

    def _set_defaults(self):
        self._conf['runtime'] = self._conf.get('runtime', np.inf)
        self._conf['stoptime'] = self._conf.get('stoptime', 5.0*60.0)
        self._left_frac = 0.5

    def run(self, state=None, silent=False):
        """run a task db"""

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

        global _taskdb
        _taskdb = self._taskdb

        self._taskdb.run()

        global _task_infos
        global _pool

        self._start_time = time.time()

        _pool = multiprocessing.Pool(processes=self._conf['n'])
        pool_res = [None for i in range(self._conf['n'])]

        finished = False

        while time.time() - self._start_time < self._conf['runtime'] - self._conf['stoptime']:
            if self._taskdb.state() == TASKDB_STATES.PAUSED:
                break

            for i in range(self._conf['n']):
                if pool_res[i] is None:
                    tsk, id = self._taskdb.checkout(state=state)

                    if tsk is None:
                        finished = True
                        break

                    pool_res[i] = _pool.apply_async(_run_work, (tsk, id, silent))
                    _task_infos.append((id, time.time(), silent))
                else:
                    try:
                        id, err = pool_res[i].get(0.1)
                        pool_res[i] = None
                        got_result = True
                    except multiprocessing.TimeoutError:
                        got_result = False
                        pass

                    if got_result:
                        for _i in range(len(_task_infos)):
                            if _task_infos[_i][0] == id:
                                break
                        del _task_infos[_i]

                        if err != 0:
                            checkin_state = TASK_STATES.FAILED
                            info = str(err)
                        else:
                            checkin_state = TASK_STATES.SUCCEEDED
                            info = ''

                        self._taskdb.checkin(id, checkin_state, info=info)

            if finished:
                break

        _pool.close()

        while (len(_task_infos) > 0 and
               time.time() - self._start_time < self._conf['runtime'] - self._conf['stoptime']*self._left_frac):

            for i in range(self._conf['n']):
                if pool_res[i] is not None:
                    try:
                        id, err = pool_res[i].get(0.1)
                        pool_res[i] = None
                        got_result = True
                    except multiprocessing.TimeoutError:
                        got_result = False
                        pass

                    if got_result:
                        for _i in range(len(_task_infos)):
                            if _task_infos[_i][0] == id:
                                break
                        del _task_infos[_i]

                        if err != 0:
                            checkin_state = TASK_STATES.FAILED
                            info = str(err)
                        else:
                            checkin_state = TASK_STATES.SUCCEEDED
                            info = ''

                        self._taskdb.checkin(id, checkin_state, info=info)

        _pool.terminate()
        _pool.join()

        for _task_info in _task_infos:
            if not _task_info[2]:
                etime = time.time()
                print_end(_task_info[0], _task_info[1], etime, TASK_STATES.KILLED)
            _taskdb.checkin(_task_info[0], TASK_STATES.KILLED)
