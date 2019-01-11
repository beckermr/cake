import sys
import numpy as np
import time
import subprocess
import signal

from .base import BaseWorker
from ..defaults import TASK_STATES, TASKDB_STATES
from ..utils import print_start, print_end

_task_info = None
_taskdb = None


def _signal_handler(signal, frame):
    if _taskdb is not None and _task_info is not None:
        if not _task_info[2]:
            etime = time.time()
            print_end(_task_info[0], _task_info[1], etime, TASK_STATES.KILLED)
        _taskdb.checkin(_task_info[0], TASK_STATES.KILLED)
    sys.exit(0)


class SerialWorker(BaseWorker):
    def _set_defaults(self):
        self._conf['runtime'] = self._conf.get('runtime', np.inf)

    def run(self, state=None, silent=False):
        """run a task db"""

        # set signal handlers
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
        global _taskdb
        _taskdb = self._taskdb

        self._taskdb.run()

        global _task_info

        self._start_time = time.time()

        while time.time() - self._start_time < self._conf['runtime']:
            if self._taskdb.state() == TASKDB_STATES.PAUSED:
                break

            tsk, id = self._taskdb.checkout(state=state)

            if tsk is None:
                break

            stime = time.time()
            if not silent:
                print_start(id, stime)

            _task_info = (id, stime, silent)

            err = subprocess.run(tsk, shell=True).returncode
            etime = time.time()

            if err != 0:
                checkin_state = TASK_STATES.FAILED
                info = str(err)
            else:
                checkin_state = TASK_STATES.SUCCEEDED
                info = ''

            self._taskdb.checkin(id, checkin_state, info=info)

            if not silent:
                print_end(id, stime, etime, checkin_state)
