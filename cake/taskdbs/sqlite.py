from __future__ import print_function
import os
import sys
import sqlite3
import time
import uuid

import yaml

from ..defaults import TASK_STATES, TASKDB_STATES, TASK_LOG_ACTIONS
from ..defaults import LIST_OF_TASK_STATES, VALID_LOG_CHECKIN_ACTIONS
from ..defaults import DEF_TASKDB_CONF
from .base import BaseTaskDB
from ..utils import diff_timestamps


class SQLiteTaskDB(BaseTaskDB):
    def __init__(self, **conf):
        self._conf = {}
        self._conf.update(conf)
        self._conf['intent'] = self._conf.get('intent', 'examine')
        self._init_db()

    def _init_db(self):
        self._errcheck = False
        self._conn = None

        if not os.path.exists(self._conf['name']):
            # create the DB
            self._conn = sqlite3.connect(self._conf['name'], isolation_level=None)
            tconf = {}
            tconf.update(DEF_TASKDB_CONF)
            tconf.update(self._conf)
            self._conf.update(tconf)

            self._create_db()
        else:
            # connect to DB on disk
            self._conn = sqlite3.connect(self._conf['name'], isolation_level=None)

            old_timeout = self._conf.get('timeout', DEF_TASKDB_CONF['timeout'])
            self._conf['timeout'] = DEF_TASKDB_CONF['timeout']
            if self._lock_db(exclusive=True, msg='init setup'):
                try:
                    old_conf = yaml.load(self._conn.execute('SELECT CONFIG FROM INFO').fetchall()[0][0])
                    self._unlock_db()
                except Exception as e:
                    self._rollback_db()
                    raise ValueError("Config could not be selected from SQLite3 DB!")
            else:
                raise ValueError("SQLite3 DB could not be locked when trying to get config!")

            self._conf['timeout'] = old_timeout

            tconf = {}
            tconf.update(DEF_TASKDB_CONF)
            tconf.update(old_conf)
            tconf.update(self._conf)
            self._conf.update(tconf)

        # add self client
        self._client_id = uuid.uuid1().hex
        self._add_client(self._client_id)

    def _create_db(self):
        if self._lock_db(exclusive=True, msg='create'):
            try:
                self._conn.execute("CREATE TABLE TASKS(\n"
                                   "TASK_ID TEXT PRIMARY KEY NOT NULL,\n"
                                   "CMD TEXT DEFAULT '',\n"
                                   "STATE TEXT DEFAULT '%s',\n"
                                   "PRIORITY REAL DEFAULT 0);\n"
                                   % TASK_STATES.QUEUED_NO_DEP)

                self._conn.execute("CREATE TABLE LOGS(\n"
                                   "LOG_ID INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                                   "ACTION TEXT DEFAULT '%s',\n"
                                   "INFO TEXT DEFAULT '',\n"
                                   "TIME TIMESTAMP DEFAULT(strftime('%%Y-%%m-%%d %%H:%%M:%%f','NOW')),\n"
                                   "TASK_ID TEXT,\n"
                                   "FOREIGN KEY(TASK_ID) REFERENCES TASKS(TASK_ID));\n"
                                   % TASK_LOG_ACTIONS.ADDED)

                self._conn.execute("CREATE TABLE INFO(\n"
                                   "STATE TEXT DEFAULT '%s',\n"
                                   "CONFIG TEXT);\n"
                                   % (TASKDB_STATES.PAUSED))

                self._conn.execute("CREATE TABLE CLIENTS(\n"
                                   "CLIENT_ID TEXT PRIMARY KEY NOT NULL);")

                self._conn.execute('INSERT INTO INFO (STATE,CONFIG) VALUES (?,?)',
                                   (TASKDB_STATES.PAUSED, yaml.dump(self._conf)))

                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("SQLite3 DB could not be created!")
        else:
            raise ValueError("SQLite3 DB could not be locked when trying to create it!")

    def _add_client(self, client_id):
        if self._lock_db(transaction=True, msg='add client'):
            try:
                self._conn.execute("INSERT INTO CLIENTS (CLIENT_ID) VALUES (?)", (client_id,))
                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Client could not be added to the SQLite3 database!")
        else:
            raise ValueError("SQLite3 databse could not be locked when trying to add a client!")

    def close(self):
        """close down the DB"""
        if self._lock_db(exclusive=True, msg='close'):
            try:
                num_clients = self._conn.execute('SELECT COUNT(*) FROM CLIENTS').fetchall()[0][0]
                self._conn.execute("DELETE FROM CLIENTS WHERE CLIENT_ID = '%s'" % self._client_id)
                if num_clients == 1:
                    self._conn.execute("UPDATE INFO SET STATE = '%s'" % TASKDB_STATES.PAUSED)
                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Could not cleanup SQLite3 DB!")
        else:
            raise ValueError("SQLite3 DB could not be locked when trying to cleanup!")

        self._conn.close()

    def _lock_db(self, transaction=False, exclusive=False, msg=''):
        """lock the DB"""
        if self._errcheck:
            print('CAKE: lock DB - %s' % msg)

        locked = False

        start = time.time()
        while time.time() - start < self._conf['timeout']:
            try:
                if exclusive:
                    self._conn.execute("BEGIN EXCLUSIVE")
                elif transaction:
                    self._conn.execute("BEGIN TRANSACTION")
                else:
                    self._conn.execute("BEGIN")

                locked = True
                break
            except Exception as e:
                pass

        return locked

    def _unlock_db(self):
        if self._errcheck:
            print('CAKE: unlock')
        self._conn.execute('commit')

    def _rollback_db(self):
        if self._errcheck:
            print('CAKE: rollback')
        self._conn.execute('rollback')

    def _write_log(self, c, task_id, action, info=''):
        c.execute("INSERT INTO LOGS(ACTION, TASK_ID, INFO) VALUES (?,?,?)",
                  (action, task_id, info))

    def _update_task_state(self, c, task_id, state):
        c.execute("UPDATE TASKS SET STATE = '%s' WHERE TASK_ID = '%s'" % (state, task_id))

    def checkout(self, state=None):
        """checkout a task from the DB"""

        if state is not None and state not in LIST_OF_TASK_STATES:
            raise ValueError("State '%s' not a valid task state!" % state)

        got_task = False
        task = None
        id = None

        for itr in range(self._conf['task_checkout_num_tries']):
            if itr > 0:
                time.sleep(self._conf['task_checkout_delay'])

            if self._lock_db(exclusive=True, msg='checkout'):
                try:
                    if state is None:
                        c = self._conn.execute("SELECT CMD, TASK_ID, STATE FROM TASKS\n"
                                               "WHERE STATE = '%s' OR STATE = '%s' OR STATE = '%s'\n"
                                               "ORDER BY PRIORITY DESC LIMIT 1;"
                                               % (TASK_STATES.QUEUED_NO_DEP,
                                                  TASK_STATES.CHECKPOINTED,
                                                  TASK_STATES.KILLED))
                    else:
                        c = self._conn.execute("SELECT CMD, TASK_ID, STATE\n"
                                               "FROM TASKS WHERE STATE = '%s'\n"
                                               "ORDER BY PRIORITY DESC LIMIT 1;"
                                               % (state))

                    tasks = c.fetchall()
                    if len(tasks) == 0:
                        self._unlock_db()
                        break
                    else:
                        task, id, state = tasks[0]

                    self._update_task_state(self._conn, id, TASK_STATES.RUNNING)

                    if state == TASK_STATES.CHECKPOINTED:
                        logval = TASK_LOG_ACTIONS.RAN_FROM_CHECKPOINT
                    else:
                        logval = TASK_LOG_ACTIONS.RAN

                    self._write_log(self._conn, id, logval)

                    self._unlock_db()

                    got_task = True
                except Exception as e:
                    self._rollback_db()
                    got_task = False
                    task = None
                    id = None
                    pass

            if got_task:
                break

        return task, id

    def checkin(self, task_id, state, info=''):
        """checkin a task that has been run"""

        if type(task_id) != str:
            task_id = str(task_id)

        assert state in VALID_LOG_CHECKIN_ACTIONS,\
            "Supplied task '%s' state is not allowed!" % state

        if self._lock_db(exclusive=True, msg='checkin'):
            try:
                self._update_task_state(self._conn, task_id, state)
                self._write_log(self._conn, task_id, state, info=info)
                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Checkin of task %s in state '%s' failed!" % (task_id, state))
        else:
            raise ValueError("Could not get lock for checkin of task %s in state '%s'!" % (task_id, state))

    def add(self, cmd, id=None, priority=None):
        """add a task to be run via cmd"""

        if id is None:
            id = uuid.uuid1().hex

        if priority is None:
            priority = 0

        state = TASK_STATES.QUEUED_NO_DEP
        log_state = TASK_LOG_ACTIONS.ADDED

        if self._lock_db(exclusive=True, msg='add'):
            try:
                c = self._conn.execute("SELECT TASK_ID FROM TASKS WHERE TASK_ID = '%s'" % id)
                if len(c.fetchall()) != 0:
                    raise ValueError("Could not add task %s in state '%s' w/ cmd '%s' due to duplicate ID!" % (
                        id, state, cmd))

                self._conn.execute("INSERT INTO TASKS(CMD, STATE, TASK_ID, PRIORITY) VALUES (?,?,?,?)",
                                   (cmd, state, id, priority))
                self._write_log(self._conn, id, log_state)
                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Could not add task %s in state '%s' w/ cmd '%s'!" % (id, state, cmd))
        else:
            raise ValueError("Could not lock DB to add task %s in state '%s' w/ cmd '%s'!" % (id, state, cmd))

        return id

    def add_multiple(self, cmds, id=None, priority=None):
        """add tasks to be run via cmd"""

        if id is None:
            id = [uuid.uuid1().hex for cmd in cmds]

        if priority is None:
            priority = [0 for cmd in cmds]
        else:
            try:
                priority = iter(priority)
            except Exception as e:
                priority = [priority for cmd in cmds]

        states = [TASK_STATES.QUEUED_NO_DEP for cmd in cmds]
        log_states = [TASK_LOG_ACTIONS.ADDED for cmd in cmds]
        infos = ['' for cmd in cmds]

        if self._lock_db(exclusive=True, msg='add multiple'):
            try:
                c = self._conn.execute("SELECT TASK_ID FROM TASKS")
                db_ids = c.fetchall()
                curr_ids = set([tup[0] for tup in db_ids])
                set_ids = set(id)
                int_ids = set_ids & curr_ids
                if len(int_ids) != 0:
                    raise ValueError("duplicate IDs found")

                self._conn.executemany("INSERT INTO TASKS(CMD, STATE, TASK_ID, PRIORITY) VALUES (?,?,?,?)",
                                       zip(cmds, states, id, priority))
                self._conn.executemany("INSERT INTO LOGS(ACTION, TASK_ID, INFO) VALUES (?,?,?)",
                                       zip(log_states, id, infos))
                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Could not add multiple tasks! - %s" % e)

        else:
            raise ValueError("Could not get lock to add multiple tasks!")

        return id

    def update(self, id, task=None, priority=None, state=None):
        """update a task with id"""

        if self._lock_db(exclusive=True, msg='update'):
            try:
                info = ''

                if priority is not None:
                    c = self._conn.execute("SELECT PRIORITY FROM TASKS WHERE TASK_ID = '%s'" % (id))
                    old_priority = c.fetchall()[0][0]
                    self._conn.execute("UPDATE TASKS SET PRIORITY = %d WHERE TASK_ID = '%s'" % (priority, id))
                    info += 'set PRIORITY to %d from %d; ' % (priority, old_priority)

                if state is not None:
                    c = self._conn.execute("SELECT STATE FROM TASKS WHERE TASK_ID = '%s'" % (id))
                    old_state = c.fetchall()[0][0]
                    self._conn.execute("UPDATE TASKS SET STATE = '%s' WHERE TASK_ID = '%s'" % (state, id))
                    info += 'set STATE to %s from %s; ' % (state, old_state)

                if task is not None:
                    c = self._conn.execute("SELECT CMD FROM TASKS WHERE TASK_ID = '%s'" % (id))
                    old_cmd = c.fetchall()[0][0]
                    self._conn.execute("UPDATE TASKS SET CMD = '%s' WHERE TASK_ID = '%s'" % (task, id))
                    info += 'set CMD to "%s" from "%s"; ' % (task, old_cmd)

                self._write_log(c, id, TASK_LOG_ACTIONS.UPDATED, info=info)
                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Could not udpate task %s!" % (id))
        else:
            raise ValueError("Could not get lock to udpate task %s!" % (id))

    def delete(self, id, remove=False):
        """delete task id"""

        state = TASK_STATES.DELETED
        log_state = TASK_LOG_ACTIONS.DELETED

        if self._lock_db(exclusive=True, msg='delete'):
            try:
                if remove:
                    self._conn.execute("DELETE FROM TASKS WHERE TASK_ID = '%s'" % id)
                else:
                    self._update_task_state(self._conn, id, state)
                self._write_log(self._conn, id, log_state)
                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Could not delete task %s!" % id)
        else:
            raise ValueError("Could not get lock to delete task %s!" % id)

    def reset(self):
        """reset all tasks to be rerun"""

        state = TASK_STATES.QUEUED_NO_DEP
        log_state = TASK_LOG_ACTIONS.RESET

        if self._lock_db(exclusive=True, msg='reset'):
            try:
                cids = self._conn.cursor()
                cids.execute("SELECT TASK_ID FROM TASKS WHERE STATE != '%s'" % TASK_STATES.DELETED)

                c = self._conn.cursor()
                while True:
                    ids = cids.fetchmany(size=c.arraysize)

                    if len(ids) == 0:
                        break

                    for id in ids:
                        self._write_log(c, id[0], log_state)

                self._conn.execute("UPDATE TASKS SET STATE = '%s' WHERE STATE != '%s'" % (state, TASK_STATES.DELETED))

                self._conn.execute("DELETE FROM CLIENTS WHERE CLIENT_ID != '%s'" % self._client_id)
                self._conn.execute("UPDATE INFO SET STATE = '%s'" % TASKDB_STATES.PAUSED)

                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Could not reset task DB!")
        else:
            raise ValueError("Could not get lock to reset task DB!")

    def _extract_runtime_task(self, logs):
        succ_tup = None
        ran_tup = None
        for i in range(len(logs) - 1, 0, -1):
            if logs[i][2] == TASK_LOG_ACTIONS.SUCCEEDED and succ_tup is None:
                succ_tup = logs[i]

            if logs[i][2] == TASK_LOG_ACTIONS.RAN and ran_tup is None:
                ran_tup = logs[i]

            if succ_tup is not None and ran_tup is not None:
                break

        return diff_timestamps(ran_tup[3], succ_tup[3])

    def list(self, state=None, with_runtime=False):
        """list all tasks in the db"""

        if state is None:
            c = self._conn.execute('SELECT TASK_ID, STATE, CMD, PRIORITY FROM TASKS ORDER BY PRIORITY DESC')
        else:
            if state not in LIST_OF_TASK_STATES:
                raise ValueError("State '%s' not a valid task state!" % state)
            c = self._conn.execute("SELECT TASK_ID, STATE, CMD, PRIORITY "
                                   "FROM TASKS WHERE STATE = '%s' ORDER BY PRIORITY DESC" % state)

        tasks = c.fetchall()
        for id, state, cmd, priority in tasks:
            print("task %s:\n    cmd: '%s'\n    state: %s\n    priority: %d" % (id, cmd, state, priority))

            if state == TASK_STATES.SUCCEEDED and with_runtime:
                c = self._conn.execute("SELECT LOG_ID, TASK_ID, ACTION, TIME, INFO "
                                       "FROM LOGS WHERE TASK_ID = '%s' ORDER BY LOG_ID ASC" % id)
                logs = c.fetchall()
                rtime = self._extract_runtime_task(logs)
                print("    run time: %gs" % rtime)

        sys.stdout.flush()

    def log(self, task_id):
        """get log for task id"""
        if type(task_id) != str:
            task_id = str(task_id)

        c = self._conn.cursor()
        c.execute("SELECT LOG_ID, TASK_ID, ACTION, TIME, INFO "
                  "FROM LOGS WHERE TASK_ID = '%s' ORDER BY LOG_ID ASC" % task_id)
        logs = c.fetchall()
        c.execute("SELECT TASK_ID, CMD, STATE, PRIORITY FROM TASKS WHERE TASK_ID = '%s'" % task_id)
        id, cmd, state, priority = c.fetchall()[0]
        print("task %s:\n    cmd: '%s'\n    state: %s\n    priority: %d" % (id, cmd, state, priority))

        if state == TASK_STATES.SUCCEEDED:
            rtime = self._extract_runtime_task(logs)
            print("    run time: %gs" % rtime)

        print("    log:")

        for logid, taskid, action, t, info in logs:
            tstr = t
            if len(info) > 0:
                print("        %s - %s - '%s'" % (tstr, action, info))
            else:
                print("        %s - %s" % (tstr, action))

        sys.stdout.flush()

    def query(self, cmd):
        """run the query cmd on the database"""
        res = None
        if self._lock_db(exclusive=True, msg='query'):
            try:
                c = self._conn.execute(cmd)
                res = c.fetchall()
                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Could not do query '%s'" % (cmd))
        else:
            raise ValueError("Could not lock DB to do query '%s'" % (cmd))

        return res

    def status(self):
        """print staus of DB"""
        maxlen = None
        for state in LIST_OF_TASK_STATES:
            slen = len("    %s: " % state)
            if maxlen is None or slen > maxlen:
                maxlen = slen

        def _pad_str(sstr):
            if len(sstr) < maxlen:
                sstr += " " * (maxlen - len(sstr))
            return sstr

        c = self._conn.cursor()

        c.execute("SELECT STATE FROM INFO")
        state = c.fetchone()[0]
        print("%s:" % (self._conf['name']))
        sstr = _pad_str("    state: ")
        print("%s%s" % (sstr, state))

        c.execute("SELECT COUNT(*) FROM CLIENTS")
        num_clients = c.fetchone()[0]
        sstr = _pad_str("    # of clients: ")
        print("%s%d" % (sstr, num_clients))

        c.execute("SELECT COUNT(*) FROM TASKS WHERE STATE != '%s'" % TASK_STATES.DELETED)
        num = c.fetchone()[0]
        sstr = _pad_str("    # of tasks: ")
        print("%s%d" % (sstr, num))

        for state in LIST_OF_TASK_STATES:
            c.execute("SELECT COUNT(*) FROM TASKS WHERE STATE = '%s'" % state)
            num = c.fetchone()[0]
            sstr = _pad_str("    %s: " % state)
            print("%s%d" % (sstr, num))

        sys.stdout.flush()

    def _set_db_state(self, state):
        if self._lock_db(exclusive=True, msg='set DB state'):
            try:
                self._conn.execute("UPDATE INFO SET STATE = '%s'" % state)
                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Could not set DB state to '%s'" % (state))
        else:
            raise ValueError("Could not lock DB to set DB state to '%s'" % (state))

    def state(self):
        """get the DB state"""
        c = self._conn.cursor()

        c.execute("SELECT STATE FROM INFO")
        state = c.fetchone()[0]
        return state

    def pause(self):
        """set DB state to pause"""
        self._set_db_state(TASKDB_STATES.PAUSED)

    def run(self):
        """set DB state to running"""
        self._set_db_state(TASKDB_STATES.RUNNING)

    def cleanup(self):
        """cleanup the taskdb"""
        if self._lock_db(exclusive=True, msg='cleanup'):
            try:
                cids = self._conn.cursor()
                cids.execute("SELECT TASK_ID FROM TASKS WHERE STATE = '%s'" % TASK_STATES.RUNNING)

                c = self._conn.cursor()
                while True:
                    ids = cids.fetchmany(size=c.arraysize)

                    if len(ids) == 0:
                        break

                    for id in ids:
                        self._write_log(c, id[0], TASK_LOG_ACTIONS.CLEANED)

                c.execute("UPDATE INFO SET STATE = '%s'" % TASKDB_STATES.PAUSED)
                c.execute("DELETE FROM CLIENTS WHERE CLIENT_ID != '%s'" % self._client_id)
                c.execute("UPDATE TASKS SET STATE = '%s' WHERE STATE = '%s'" % (
                    TASK_STATES.KILLED, TASK_STATES.RUNNING))

                self._unlock_db()
            except Exception as e:
                self._rollback_db()
                raise ValueError("Could not cleanup the task DB!")
        else:
            raise ValueError("Could not lock DB to cleanup the task DB!")

    def runtime(self):
        """get runtime stats for DB"""
        from itertools import groupby

        c = self._conn.cursor()
        c.execute("""\
SELECT tl.task_id, tl.log_id, tl.action, tl.time
FROM LOGS as tl, TASKS as t
WHERE t.task_id = tl.task_id and t.state = 'SUCCEEDED'
ORDER BY tl.task_id, tl.log_id DESC""")
        all_logs = c.fetchall()

        num_tasks = 0
        max_task = None
        max_time = None
        min_time = None
        min_task = None
        tot_time = 0.0

        for id, ilogs in groupby(all_logs, lambda x: x[0]):
            logs = list(ilogs)

            stime = None
            slog_id = None
            etime = None
            elog_id = None
            rlog_id = None
            for _, log_id, state, _time in logs:
                if state == TASK_LOG_ACTIONS.SUCCEEDED:
                    etime = _time
                    elog_id = log_id
                elif state == TASK_LOG_ACTIONS.RAN:
                    stime = _time
                    slog_id = log_id
                elif state == TASK_LOG_ACTIONS.DELETED or \
                        state == TASK_LOG_ACTIONS.RESET or \
                        state == TASK_LOG_ACTIONS.KILLED or \
                        state == TASK_LOG_ACTIONS.UPDATED or \
                        state == TASK_LOG_ACTIONS.ADDED:
                    rlog_id = log_id
                    break

            if slog_id > rlog_id and elog_id > rlog_id:
                tdiff = diff_timestamps(stime, etime)

                if max_time is None or tdiff > max_time:
                    max_time = tdiff
                    max_task = id

                if min_time is None or tdiff < min_time:
                    min_time = tdiff
                    min_task = id

                num_tasks += 1
                tot_time += tdiff

        if num_tasks > 0:
            avg_time = tot_time / num_tasks
        else:
            avg_time = 0.0

        if max_task is None:
            max_task = 'N/A'
            max_time = 0.0

        if min_task is None:
            min_task = 'N/A'
            min_time = 0.0

        print("%s:" % (self._conf['name']))
        print("""\
    total time:           %gs
    # of tasks SUCCEEDED: %d
    avg time per task:    %gs
    min time of tasks:    %gs (task %s)
    max time of tasks:    %gs (task %s)""" % (
            tot_time, num_tasks, avg_time, min_time, min_task, max_time, max_task))

        sys.stdout.flush()
