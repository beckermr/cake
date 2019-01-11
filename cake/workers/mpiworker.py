import sys
import signal
import numpy as np
import time
import subprocess
from mpi4py import MPI

from .base import BaseWorker
from ..defaults import TASKDB_STATES, TASK_STATES
from ..utils import print_start, print_end

_comm = None
_task_infos = []
_taskdb = None

# tags
STOP_WORK = 10
RECV_WORK = 11
READY_WORKER = 12
RESULTS_WORKER = 13
KILLED_WORKER = 14


def _worker_signal_handler(signal, frame):
    if _comm is not None:
        _comm.send(None, dest=0, tag=KILLED_WORKER)
    sys.exit(0)


def _master_signal_handler(signal, frame):
    if _taskdb is not None:
        for _task_info in _task_infos:
            print(_task_info, flush=True)
            if not _task_info[2]:
                etime = time.time()
                print_end(_task_info[0], _task_info[1], etime, TASK_STATES.KILLED)
            _taskdb.checkin(_task_info[0], TASK_STATES.KILLED)
    sys.exit(0)


class MPIWorker(BaseWorker):
    def __init__(self, **conf):
        self._conf = {}
        self._conf.update(conf)
        self._set_defaults()

        if self._conf['master']:
            self._init_mpi_master()
        else:
            self._init_mpi_worker()

        if self._rank == 0:
            assert 'taskdb_conf' in conf, "The TaskDB config must be given to MPI worker!"
            assert 'taskdb_class' in conf, "The TaskDB class must be given to MPI worker!"
            self._taskdb = conf['taskdb_class'](**conf['taskdb_conf'])

    def _set_defaults(self):
        self._conf['runtime'] = self._conf.get('runtime', np.inf)
        self._conf['spawn_master'] = self._conf.get('spawn_master', False)
        self._conf['master'] = self._conf.get('master', False)
        self._conf['stoptime'] = self._conf.get('stoptime', 5.0 * 60.0)
        self._left_frac = 0.5

    def close(self):
        for comm in self._comms:
            comm.Disconnect()
            self._comms.remove(comm)
        if self._rank == 0:
            self._taskdb.close()

    def _init_mpi_master(self):
        self._comms = []
        if self._conf['spawn_master']:
            self._comms.append(MPI.Comm.Get_parent())
            mcomm = self._comms[-1].Merge(high=True)
            self._comms.append(mcomm)
            self._comm = mcomm
        else:
            self._comm = MPI.COMM_WORLD

        self._rank = self._comm.Get_rank()
        self._size = self._comm.Get_size()

    def _init_mpi_worker(self):
        self._comms = []
        if self._conf['spawn_master']:
            args = []
            for arg in sys.argv:
                args.append(arg)
            args.append('--master')
            self._comms.append(MPI.COMM_WORLD.Spawn(sys.executable,
                                                    args=args,
                                                    maxprocs=1))
            mcomm = self._comms[-1].Merge(high=True)
            self._comms.append(mcomm)
            self._comm = mcomm
        else:
            self._comm = MPI.COMM_WORLD

        global _comm
        _comm = self._comm

        self._rank = self._comm.Get_rank()
        self._size = self._comm.Get_size()

    def run(self, state=None, silent=False):
        """run a task db"""
        if self._rank == 0:
            self._run_master(state=state, silent=silent)
        else:
            self._run_worker(silent=silent)

    def _run_worker(self, silent=False):
        signal.signal(signal.SIGTERM, _worker_signal_handler)
        signal.signal(signal.SIGINT, _worker_signal_handler)

        status = MPI.Status()
        while True:
            self._comm.send(-1, dest=0, tag=READY_WORKER)
            res = self._comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()

            if tag == STOP_WORK:
                break
            elif tag == RECV_WORK:
                tsk, id = res

                if not silent:
                    stime = time.time()
                    print_start(id, stime)

                err = subprocess.call(tsk, shell=True)

                if not silent:
                    etime = time.time()
                    if err != 0:
                        print_end(id, stime, etime, TASK_STATES.FAILED)
                    else:
                        print_end(id, stime, etime, TASK_STATES.SUCCEEDED)

                self._comm.send((err, id), dest=0, tag=RESULTS_WORKER)

    def _run_master(self, state=None, silent=False):
        signal.signal(signal.SIGTERM, _master_signal_handler)
        signal.signal(signal.SIGINT, _master_signal_handler)

        global _taskdb
        _taskdb = self._taskdb

        # init db, time, etc.
        self._taskdb.run()
        global _task_infos
        self._start_time = time.time()
        status = MPI.Status()

        # do work
        while time.time() - self._start_time < self._conf['runtime'] - self._conf['stoptime']:
            if self._taskdb.state() == TASKDB_STATES.PAUSED:
                break

            res = self._comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()
            worker = status.Get_source()

            if tag == READY_WORKER:
                tsk, id = self._taskdb.checkout(state=state)
                if tsk is None:
                    break

                _task_infos.append((id, time.time(), silent))
                self._comm.send((tsk, id), dest=worker, tag=RECV_WORK)
            elif tag == RESULTS_WORKER:
                err, id = res
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

        # get rest of results
        while (len(_task_infos) > 0 and
               time.time() - self._start_time < self._conf['runtime'] - self._conf['stoptime'] * self._left_frac):
            res = self._comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()
            worker = status.Get_source()

            if tag == RESULTS_WORKER:
                err, id = res
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

        # stop workers
        for worker in range(1, self._size):
            self._comm.send(-1, dest=worker, tag=STOP_WORK)

        # mark rest as failed
        for _task_info in _task_infos:
            if not _task_info[2]:
                etime = time.time()
                print_end(_task_info[0], _task_info[1], etime, TASK_STATES.KILLED)
            _taskdb.checkin(_task_info[0], TASK_STATES.KILLED)
