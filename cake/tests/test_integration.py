import os
import time
import subprocess

import pytest

from cake import SQLiteTaskDB


@pytest.fixture(params=[SQLiteTaskDB])
def taskdb(request):
    name = 'test.db'
    try:
        os.remove(name)
    except Exception as e:
        pass
    yield (name, request.param)
    try:
        os.remove(name)
    except Exception as e:
        pass


@pytest.fixture()
def tasks(request):
    name = 'tasks.txt'
    yield name
    try:
        os.remove(name)
    except Exception as e:
        pass


def test_status(taskdb):
    """test that status works"""
    with taskdb[1](name=taskdb[0]) as db:
        for i in range(16):
            db.add('echo %d' % i)

    output = subprocess.run('cake status %s' % taskdb[0],
                            shell=True,
                            check=True,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    assert 'QUEUED_NO_DEP: 16' in output, "Status of tasks was not reported correctly!"


def test_add_pipe(taskdb):
    """test adding tests via a pipe"""

    subprocess.run('seq 0 15 | xargs -I{} sh -c \'echo "echo {}"\' | cake add %s' % taskdb[0],
                   shell=True,
                   check=True)
    output = subprocess.run('cake status %s' % taskdb[0],
                            shell=True,
                            check=True,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    assert 'QUEUED_NO_DEP: 16' in output, "Tasks did not get added correctly!"


def test_add_stdin(taskdb, tasks):
    """test adding tests via a stdin"""

    with open(tasks, 'w') as fp:
        for i in range(16):
            fp.write('echo %d\n' % i)
    subprocess.run('cake add %s < %s' % (taskdb[0], tasks),
                   shell=True,
                   check=True)
    output = subprocess.run('cake status %s' % taskdb[0],
                            shell=True,
                            check=True,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    assert 'QUEUED_NO_DEP: 16' in output, "Tasks did not get added correctly!"


def test_add_file(taskdb, tasks):
    """test adding tests via a file"""

    with open(tasks, 'w') as fp:
        for i in range(16):
            fp.write('echo %d\n' % i)
    subprocess.run('cake add --file %s %s' % (tasks, taskdb[0]),
                   shell=True,
                   check=True)
    output = subprocess.run('cake status %s' % taskdb[0],
                            shell=True,
                            check=True,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    assert 'QUEUED_NO_DEP: 16' in output, "Tasks did not get added correctly!"


@pytest.mark.parametrize("arg", ["", "-n 4"])
def test_run(taskdb, arg):
    """make sure basic running works"""

    with taskdb[1](name=taskdb[0]) as db:
        for i in range(16):
            db.add('echo %d' % i)

    subprocess.run('cake run %s %s' % (arg, taskdb[0]),
                   shell=True,
                   check=True)
    output = subprocess.run('cake status %s' % taskdb[0],
                            shell=True,
                            check=True,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    assert 'SUCCEEDED:     16' in output, "Tasks did not run correctly!"


@pytest.mark.parametrize("arg,num_tasks", [("", 1), ("-n 4", 4)])
def test_killed(taskdb, arg, num_tasks):
    """test to make sure tasks get marked as killed if interupted"""

    with taskdb[1](name=taskdb[0]) as db:
        for i in range(num_tasks):
            db.add('echo %d && sleep 10' % i)

    proc = subprocess.Popen(['cake', 'run', '--stoptime', '0'] +
                            ([arg, taskdb[0]] if len(arg) > 0 else [taskdb[0]]))
    time.sleep(2)
    proc.terminate()
    proc.wait()
    output = subprocess.run('cake status %s' % taskdb[0],
                            shell=True,
                            check=True,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    assert 'KILLED:        %d' % num_tasks in output, "Tasks were not killed correctly!"


@pytest.mark.parametrize("arg,num_tasks", [("", 1), ("-n 4", 4)])
def test_pause(taskdb, arg, num_tasks):
    """test pausing"""

    with taskdb[1](name=taskdb[0]) as db:
        for i in range(num_tasks):
            db.add('echo %d && sleep 5' % i)

    proc = subprocess.Popen('cake run --stoptime 10 %s %s' % (arg, taskdb[0]), shell=True)
    time.sleep(2)
    subprocess.run('cake pause %s' % taskdb[0], shell=True, check=True)
    proc.wait()
    output = subprocess.run('cake status %s' % taskdb[0],
                            shell=True,
                            check=True,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    assert 'SUCCEEDED:     %d' % num_tasks in output, "Tasks were not paused correctly!"
