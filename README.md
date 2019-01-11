# cake

featherweight task management for high throughput computing

`cake` stores all tasks in a database (SQLite currently) on disk. Workers then request work
from the database (i.e. `checkout`) and then report the status of the work after they have finished
(i.e. `checkin`). `cake` also provides the ability to inspect (e.g., logs of actions taken for a
given task, the ability to query the underlying DB, a status summary of the DB, etc.) and to retry/rerun/reset
an entire task DB.

examples
--------
You use `cake` to load tasks like this
```python
#!/usr/bin/env python
import cake

with cake.SQLiteTaskDB(name='test.db') as db:
    for i in range(100):
        db.add('echo %d && sleep 1' % (i))
```
or directly on the command line like this
```bash
seq 0 99 | xargs -I{} sh -c 'echo "echo {} && sleep 1"' | cake add test.db
```
The method `add_multiple` is generally faster for adding a large number of tasks.

You can also add tasks directly from the command line, either from a file, as an argument like or from a pipe/stdin like so
```bash
cake add test.db "echo 234 && sleep 4"
cake add test.db --file=<...>
cat tasks.txt | cake add test.db
cake add test.db < tasks.txt
```

`cake` then has a command line utility to run tasks from a task DB like this
```bash
cake run test.db
```
The database of tasks locks (assuming the underlying filesystem locks files properly), so that more than one thread can execute tasks from the DB at the
same time.

Alternatively, `cake` can run task DBs in parallel using python multiprocessing like this
```bash
cake run -n <number of threads> test.db
```
Internally, only one python process accesses the task DB when using multiprocessing.

For large compute clusters, `cake` can be used with MPI like this
```bash
mpirun -np <number of workers> cake run test.db --mpi
```
By passing `--mpi`, cake internally has only one MPI task request work from the DB
(and then distribute this work to the rest of the MPI tasks) in order to limit the number of threads
attempting to read from the DB simultaneously.

If you also pass the flag `--spawn-master`, `cake` will use MPI-2 support for dynamic tasks to have the workers
spawn their own master. Otherwise, if say 10 MPI tasks are used with the `--mpi` flag, only 9 of them actually
run tasks from the task DB.

To pause a running task DB, simply type
```bash
cake pause test.db
```
This command marks the DB as paused internally. Any running instance of `cake` will see this and exit cleanly after all
of its current tasks complete.

The command line interface (and each corresponding object method of the DB class) lets you examine
the state of a given set of tasks like so
```bash
cake list test.db   # list all tasks in the DB
cake status test.db # print out the status of the DB
cake log test.db --task-id=<...> # print out a log of actions taken for a given task
```

See the help page for `cake` for full details
```bash
cake -h
```
