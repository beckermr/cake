from __future__ import print_function
import sys

import click
import numpy as np

from cake import SQLiteTaskDB, SerialWorker, PMPWorker, TASK_STATES
from cake.defaults import DEF_TASKDB_CONF


def _get_state(state):
    if state is not None:
        return getattr(TASK_STATES, state.upper())
    else:
        return None


@click.group()
def cli():
    """featherweight task management for high throughput computing"""
    pass


@cli.command()
@click.argument('database')
@click.option('--state', default=None, help="only select from tasks with this state")
@click.option('--runtime', default=np.inf, type=float, help="maximum runtime in seconds")
@click.option('--timeout', default=DEF_TASKDB_CONF['timeout'], type=float,
              help="timeout for locking task DB in seconds")
@click.option('--task-checkout-delay', default=DEF_TASKDB_CONF['task_checkout_delay'], type=float,
              help="time in seconds between successive attempts to checkout a task")
@click.option('--task-checkout-num-tries', default=DEF_TASKDB_CONF['task_checkout_num_tries'], type=int,
              help="number of time to attempt to checkout a task before raising an error")
@click.option('-n', default=None, type=int,
              help="number of tasks to run in parallel with python multiprocessing")
@click.option('--silent', is_flag=True, help="suppress cake log messages in stderr")
@click.option('--mpi', is_flag=True, help="run tasks with a set of MPI workers")
@click.option('--spawn-master', is_flag=True,
              help="spawn the master MPI task dynamically (may not be supported on all systems)")
@click.option('--stoptime', default=5.0 * 60.0, type=float,
              help="time in seconds of the runtime to allow the code to exit nicely")
@click.option('--master', is_flag=True,
              help="used with --spawn-master internally by the code to spawn the master task "
              "(for internal use only, never set by hand!)")
def run(database, state, runtime, timeout, task_checkout_delay, task_checkout_num_tries, n, silent,
        mpi, spawn_master, stoptime, master):
    """run tasks in DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['timeout'] = timeout
    conf['task_checkout_delay'] = task_checkout_delay
    conf['task_checkout_num_tries'] = task_checkout_num_tries
    conf['intent'] = 'run'
    state = _get_state(state)

    workerconf = {'taskdb_conf': conf,
                  'taskdb_class': SQLiteTaskDB,
                  'runtime': runtime}

    if mpi:
        from cake.workers.mpiworker import MPIWorker
        workerconf.update({
           'spawn_master': spawn_master,
           'master': master,
           'stoptime': stoptime})
        with MPIWorker(**workerconf) as w:
            w.run(state=state, silent=silent)
    elif n is not None:
        workerconf.update({
           'stoptime': stoptime,
           'n': n})
        with PMPWorker(**workerconf) as w:
            w.run(state=state, silent=silent)
    else:
        with SerialWorker(**workerconf) as w:
            w.run(state=state, silent=silent)


@cli.command()
@click.pass_context
@click.argument('database')
@click.option('--runtime', default=np.inf, type=float, help="maximum runtime in seconds")
@click.option('--timeout', default=DEF_TASKDB_CONF['timeout'], type=float,
              help="timeout for locking task DB in seconds")
@click.option('--task-checkout-delay', default=DEF_TASKDB_CONF['task_checkout_delay'], type=float,
              help="time in seconds between successive attempts to checkout a task")
@click.option('--task-checkout-num-tries', default=DEF_TASKDB_CONF['task_checkout_num_tries'], type=int,
              help="number of time to attempt to checkout a task before raising an error")
@click.option('-n', default=None, type=int,
              help="number of tasks to run in parallel with python multiprocessing")
@click.option('--silent', is_flag=True, help="suppress cake log messages in stderr")
@click.option('--mpi', is_flag=True, help="run tasks with a set of MPI workers")
@click.option('--spawn-master', is_flag=True,
              help="spawn the master MPI task dynamically (may not be supported on all systems)")
@click.option('--stoptime', default=5.0 * 60.0, type=float,
              help="time in seconds of the runtime to allow the code to exit nicely")
@click.option('--master', is_flag=True,
              help="used with --spawn-master internally by the code to spawn the master task "
              "(for internal use only, never set by hand!)")
def retry(ctx, database, runtime, timeout, task_checkout_delay, task_checkout_num_tries, n, silent,
          mpi, spawn_master, stoptime, master):
    """rerun failed tasks in DATABASE"""
    ctx.forward(run, state='failed')


@cli.command()
@click.argument('database')
@click.option('--state', default=None, help="only select from tasks with this state")
@click.option('--with-runtime', is_flag=True, help="also list runtime for each task")
def list(database, state, with_runtime):
    """list tasks in DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'
    state = _get_state(state)
    with SQLiteTaskDB(**conf) as taskdb:
        taskdb.list(state=state, with_runtime=with_runtime)


@cli.command()
@click.argument('database')
@click.argument('taskid')
@click.option('--remove', is_flag=True,
              help="completely remove the task from the database instead of just marking it DELETED")
def delete(database, taskid, remove):
    """delete TASKID from the DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'
    with SQLiteTaskDB(**conf) as taskdb:
        taskdb.delete(taskid, remove=remove)


@cli.command()
@click.argument('database')
@click.argument('taskid')
@click.option('--state', default=None, help="state to set task to")
@click.option('--task', default=None, help="command to run for task")
@click.option('--priority', default=None, type=int, help="priority to set for task")
def update(database, taskid, state, task, priority):
    """update properties of TASKID in DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'
    state = _get_state(state)
    with SQLiteTaskDB(**conf) as taskdb:
        taskdb.update(taskid, state=state, priority=priority, task=task)


@cli.command()
@click.argument('database')
@click.argument('args', nargs=-1)
@click.option('--file', 'filename', default=None, help="file with tasks")
@click.option('--task-id', default=None, help="id of task to add")
@click.option('--priority', default=None, type=int, help="priority to set for task")
def add(database, args, filename, task_id, priority):
    """add tasks to DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'
    with SQLiteTaskDB(**conf) as taskdb:
        if len(args) > 0:
            taskid = taskdb.add(" ".join(args[2:]), id=task_id, priority=priority)
            click.echo(taskid)

        if filename:
            tsks = []
            with open(filename, 'r') as fp:
                for line in fp:
                    tsk = line.strip()
                    if len(tsk) > 0:
                        tsks.append(tsk)
            taskids = taskdb.add_multiple(tsks, priority=priority)
            for taskid in taskids:
                click.echo(taskid)

        if not sys.stdin.isatty():
            lines = sys.stdin.readlines()
            tsks = []
            for line in lines:
                tsk = line.strip()
                if len(tsk) > 0:
                    tsks.append(tsk)

        taskids = taskdb.add_multiple(tsks, priority=priority)
        for taskid in taskids:
            click.echo(taskid)


@cli.command()
@click.argument('database')
@click.argument('taskid')
def log(database, taskid):
    """print logs for task TASKID in DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'
    with SQLiteTaskDB(**conf) as taskdb:
        taskdb.log(taskid)


@cli.command()
@click.argument('database')
def status(database):
    """print status of all tasks in DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'

    with SQLiteTaskDB(**conf) as taskdb:
        taskdb.status()


@cli.command()
@click.argument('database')
def reset(database):
    """reset all tasks in DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'
    with SQLiteTaskDB(**conf) as taskdb:
        taskdb.reset()


@cli.command()
@click.argument('database')
def runtime(database):
    """print runtime of all tasks in DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'
    with SQLiteTaskDB(**conf) as taskdb:
        taskdb.runtime()


@cli.command()
@click.argument('database')
def state(database):
    """print the state of DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'
    with SQLiteTaskDB(**conf) as taskdb:
        print(taskdb.state())


@cli.command()
@click.argument('database')
def pause(database):
    """pause the DATABASE"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'
    with SQLiteTaskDB(**conf) as taskdb:
        taskdb.pause()


@cli.command()
@click.argument('database')
def cleanup(database):
    """cleanup the DATABASE after poorly exited run"""
    conf = {'name': database}
    conf.update(DEF_TASKDB_CONF)
    conf['intent'] = 'examine'
    with SQLiteTaskDB(**conf) as taskdb:
        taskdb.cleanup()
