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


def test_run_mpi(taskdb):
    """make sure basic running works"""

    with taskdb[1](name=taskdb[0]) as db:
        for i in range(16):
            db.add('echo %d' % i)

    subprocess.run(['mpirun', '-np', '4', 'cake', 'run', '--mpi', taskdb[0]], check=True)
    output = subprocess.run('cake status %s' % taskdb[0],
                            shell=True,
                            check=True,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    assert 'SUCCEEDED:     16' in output, "Tasks did not run correctly!"


@pytest.mark.xfail
def test_killed(taskdb):
    """test to make sure tasks get marked as killed if interupted"""

    with taskdb[1](name=taskdb[0]) as db:
        for i in range(1):
            db.add('echo %d && sleep 10' % i)

    proc = subprocess.Popen(['mpirun', '-np', '2', 'cake', 'run', '--mpi', taskdb[0]])
    time.sleep(5)
    proc.terminate()
    proc.wait()
    output = subprocess.run('cake status %s' % taskdb[0],
                            shell=True,
                            check=True,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    assert 'KILLED:        1' in output, "Tasks were not killed correctly!"


def test_pause(taskdb):
    """test pausing"""

    with taskdb[1](name=taskdb[0]) as db:
        for i in range(4):
            db.add('echo %d && sleep 5' % i)

    proc = subprocess.Popen(['mpirun', '-np', '5', 'cake', 'run', '--mpi', '--stoptime', '10', taskdb[0]])
    time.sleep(2)
    subprocess.run('cake pause %s' % taskdb[0], shell=True, check=True)
    proc.wait()
    output = subprocess.run('cake status %s' % taskdb[0],
                            shell=True,
                            check=True,
                            stdout=subprocess.PIPE).stdout.decode('utf-8')
    assert 'SUCCEEDED:     4' in output, "Tasks were not paused correctly!"
