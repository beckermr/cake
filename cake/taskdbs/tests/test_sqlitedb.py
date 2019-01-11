import os
import unittest
import random
import time

from .. import SQLiteTaskDB
from ...defaults import TASK_STATES, TASK_LOG_ACTIONS, TASKDB_STATES, VALID_LOG_CHECKIN_ACTIONS


class TestSQLiteTaskDB(unittest.TestCase):
    def setUp(self):
        try:
            os.remove('test.db')
        except Exception as e:
            pass

        self._conf = {'name': 'test.db', 'timeout': 120.0}
        self._db = SQLiteTaskDB(**self._conf)

        self._delay = 0.0

    def tearDown(self):
        try:
            os.remove('test.db')
        except Exception as e:
            pass

    def test_add_log(self):
        num_add = 13
        ids = []
        priors = []
        for i in range(num_add):
            priors.append(random.uniform(0, 1))
            ids.append(self._db.add('echo "%d"' % i, priority=priors[-1]))

        for i in range(num_add):
            res = self._db.query("select priority, cmd, task_id, state from tasks where task_id = '%s'" % ids[i])[0]
            self.assertAlmostEqual(res[0], priors[i])
            self.assertTrue(res[1] == 'echo "%d"' % i)
            self.assertTrue(res[2] == ids[i])
            self.assertTrue(res[3] == TASK_STATES.QUEUED_NO_DEP)

            res = self._db.query("select task_id, action from logs where task_id = '%s'" % ids[i])[0]
            self.assertTrue(res[0] == ids[i])
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.ADDED)

    def test_add_multiple_log(self):
        num_add = 13
        priors = []
        tsks = []
        ids = []
        sids = []
        tids = []
        for i in range(num_add):
            priors.append(random.uniform(0, 1))
            tsks.append('echo "%d"' % i)
            ids.append('%d' % (i + 1000))
            sids.append('%d' % (i + 10000))
            tids.append('%d' % (i + 100000))

        for priority, aids in zip([priors, 5.1234, None, priors, 5.1234, None],
                                  [ids, sids, tids, None, None, None]):
            rids = self._db.add_multiple(tsks, priority=priority, id=aids)
            for i in range(num_add):
                res = self._db.query("select priority, cmd, task_id, state from tasks where task_id = '%s'"
                                     % rids[i])[0]
                if type(priority) == list:
                    self.assertAlmostEqual(res[0], priority[i])
                elif priority is not None:
                    self.assertAlmostEqual(res[0], priority)
                else:
                    self.assertAlmostEqual(res[0], 0.0)
                self.assertTrue(res[1] == 'echo "%d"' % i)
                self.assertTrue(res[2] == rids[i])
                self.assertTrue(res[3] == TASK_STATES.QUEUED_NO_DEP)

                res = self._db.query("select task_id, action from logs where task_id = '%s'" % rids[i])[0]
                self.assertTrue(res[0] == rids[i])
                self.assertTrue(res[1] == TASK_LOG_ACTIONS.ADDED)

    def test_add_dup(self):
        num_add = 1
        ids = []
        priors = []
        for i in range(num_add):
            priors.append(random.uniform(0, 1))
            ids.append(self._db.add('echo "%d"' % i, priority=priors[-1], id='dup'))

        try:
            self._db.add('echo "%d"' % i, priority=priors[-1], id='dup')
            failed = False
        except Exception as e:
            failed = True

        self.assertTrue(failed)

    def test_update_log(self):
        num_add = 13
        ids = []
        priors = []
        for i in range(num_add):
            priors.append(random.uniform(0, 1))
            ids.append(self._db.add('echo "%d"' % i, priority=priors[-1]))

        for i in range(num_add):
            res = self._db.query("select priority, cmd, task_id, state from tasks where task_id = '%s'" % ids[i])[0]
            self.assertAlmostEqual(res[0], priors[i])
            self.assertTrue(res[1] == 'echo "%d"' % i)
            self.assertTrue(res[2] == ids[i])
            self.assertTrue(res[3] == TASK_STATES.QUEUED_NO_DEP)

            res = self._db.query("select task_id, action from logs where task_id = '%s'" % ids[i])[0]
            self.assertTrue(res[0] == ids[i])
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.ADDED)

        for i in range(num_add):
            self._db.update(ids[i], priority=1234)
            res = self._db.query("select priority, cmd, task_id, state from tasks where task_id = '%s'" % ids[i])[0]
            self.assertAlmostEqual(res[0], 1234)
            self.assertTrue(res[1] == 'echo "%d"' % i)
            self.assertTrue(res[2] == ids[i])
            self.assertTrue(res[3] == TASK_STATES.QUEUED_NO_DEP)

            res = self._db.query("select task_id, action from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % ids[i])[0]
            self.assertTrue(res[0] == ids[i])
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.UPDATED)

        for i in range(num_add):
            self._db.update(ids[i], state=TASK_STATES.CHECKPOINTED, priority=3456)
            res = self._db.query("select priority, cmd, task_id, state from tasks where task_id = '%s'" % ids[i])[0]
            self.assertAlmostEqual(res[0], 3456)
            self.assertTrue(res[1] == 'echo "%d"' % i)
            self.assertTrue(res[2] == ids[i])
            self.assertTrue(res[3] == TASK_STATES.CHECKPOINTED)

            res = self._db.query("select task_id, action from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % ids[i])[0]
            self.assertTrue(res[0] == ids[i])
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.UPDATED)

        for i in range(num_add):
            self._db.update(ids[i], state=TASK_STATES.FAILED, task="echo -1")
            res = self._db.query("select priority, cmd, task_id, state from tasks where task_id = '%s'" % ids[i])[0]
            self.assertAlmostEqual(res[0], 3456)
            self.assertTrue(res[1] == 'echo -1')
            self.assertTrue(res[2] == ids[i])
            self.assertTrue(res[3] == TASK_STATES.FAILED)

            res = self._db.query("select task_id, action from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % ids[i])[0]
            self.assertTrue(res[0] == ids[i])
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.UPDATED)

        for i in range(num_add):
            self._db.update(ids[i], task="echo -2")
            res = self._db.query("select priority, cmd, task_id, state from tasks where task_id = '%s'" % ids[i])[0]
            self.assertAlmostEqual(res[0], 3456)
            self.assertTrue(res[1] == 'echo -2')
            self.assertTrue(res[2] == ids[i])
            self.assertTrue(res[3] == TASK_STATES.FAILED)

            res = self._db.query("select task_id, action from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % ids[i])[0]
            self.assertTrue(res[0] == ids[i])
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.UPDATED)

        for i in range(num_add):
            self._db.update(ids[i], state=TASK_STATES.DELETED, task="echo -3", priority=6789)
            res = self._db.query("select priority, cmd, task_id, state from tasks where task_id = '%s'" % ids[i])[0]
            self.assertAlmostEqual(res[0], 6789)
            self.assertTrue(res[1] == 'echo -3')
            self.assertTrue(res[2] == ids[i])
            self.assertTrue(res[3] == TASK_STATES.DELETED)

            res = self._db.query("select task_id, action from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % ids[i])[0]
            self.assertTrue(res[0] == ids[i])
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.UPDATED)

    def test_delete(self):
        num_add = 2
        ids = []
        priors = []
        for i in range(num_add):
            priors.append(random.uniform(0, 1))
            ids.append(self._db.add('echo "%d"' % i, priority=priors[-1]))

        time.sleep(self._delay)

        i = 0
        self._db.delete(ids[i], remove=False)
        res = self._db.query("select priority, cmd, task_id, state from tasks where task_id = '%s'" % ids[i])[0]
        self.assertAlmostEqual(res[0], priors[i])
        self.assertTrue(res[1] == 'echo "%d"' % i)
        self.assertTrue(res[2] == ids[i])
        self.assertTrue(res[3] == TASK_STATES.DELETED)

        res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                             % ids[i])
        res = res[0]
        self.assertTrue(res[0] == ids[i])
        self.assertTrue(res[1] == TASK_LOG_ACTIONS.DELETED)

        i = 1
        self._db.delete(ids[i], remove=True)
        res = self._db.query("select priority, cmd, task_id, state from tasks where task_id = '%s'" % ids[i])
        self.assertTrue(len(res) == 0)

        res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                             % ids[i])[0]
        self.assertTrue(res[0] == ids[i])
        self.assertTrue(res[1] == TASK_LOG_ACTIONS.DELETED)

    def test_db_state(self):
        for i in range(100):
            if random.uniform(0, 1) < 0.5:
                self._db.run()
                self.assertTrue(self._db.state() == TASKDB_STATES.RUNNING)
            else:
                self._db.pause()
                self.assertTrue(self._db.state() == TASKDB_STATES.PAUSED)

        numc = self._db.query('SELECT COUNT(*) FROM CLIENTS')[0][0]
        self.assertTrue(numc == 1)

        db2 = SQLiteTaskDB(**self._conf)
        numc = self._db.query('SELECT COUNT(*) FROM CLIENTS')[0][0]
        self.assertTrue(numc == 2)

        numc = self._db.query('SELECT COUNT(*) FROM CLIENTS')[0][0]
        self.assertTrue(numc == 2)

        db2.close()

        numc = self._db.query('SELECT COUNT(*) FROM CLIENTS')[0][0]
        self.assertTrue(numc == 1)

        with SQLiteTaskDB(**self._conf) as db2:
            numc = self._db.query('SELECT COUNT(*) FROM CLIENTS')[0][0]
            self.assertTrue(numc == 2)

            numc = self._db.query('SELECT COUNT(*) FROM CLIENTS')[0][0]
            self.assertTrue(numc == 2)

        numc = self._db.query('SELECT COUNT(*) FROM CLIENTS')[0][0]
        self.assertTrue(numc == 1)

        self._db.pause()

    def test_reset(self):
        num_add = 10
        ids = []
        priors = []
        for i in range(num_add):
            priors.append(random.uniform(0, 1))
            ids.append(self._db.add('echo "%d"' % i, priority=priors[-1]))

        time.sleep(self._delay)

        for i in range(num_add):
            tsk, id = self._db.checkout()

            res = self._db.query("select task_id, state from tasks where task_id = '%s'" % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_STATES.RUNNING)

            res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.RAN or res[1] == TASK_LOG_ACTIONS.RAN_FROM_CHECKPOINT)

            time.sleep(self._delay)

            self._db.checkin(id, VALID_LOG_CHECKIN_ACTIONS[i % len(VALID_LOG_CHECKIN_ACTIONS)])

            res = self._db.query("select task_id, state from tasks where task_id = '%s'" % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == VALID_LOG_CHECKIN_ACTIONS[i % len(VALID_LOG_CHECKIN_ACTIONS)])

            res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == VALID_LOG_CHECKIN_ACTIONS[i % len(VALID_LOG_CHECKIN_ACTIONS)])

            time.sleep(self._delay)

        self._db.reset()

        time.sleep(self._delay)

        for i in range(num_add):
            res = self._db.query("select task_id, state from tasks where task_id = '%s'" % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_STATES.QUEUED_NO_DEP or res[1] == TASK_STATES.DELETED)

            res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.RESET)

    def test_cleanup(self):
        num_add = 10
        ids = []
        priors = []
        for i in range(num_add):
            priors.append(random.uniform(0, 1))
            ids.append(self._db.add('echo "%d"' % i, priority=priors[-1]))

        time.sleep(self._delay)

        self._db.run()

        for i in range(num_add):
            tsk, id = self._db.checkout()

            res = self._db.query("select task_id, state from tasks where task_id = '%s'" % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_STATES.RUNNING)

            res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.RAN or res[1] == TASK_LOG_ACTIONS.RAN_FROM_CHECKPOINT)

            time.sleep(self._delay)

        self._db.cleanup()

        time.sleep(self._delay)

        self.assertTrue(self._db.state() == 'PAUSED')

        for i in range(num_add):
            res = self._db.query("select task_id, state from tasks where task_id = '%s'" % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_STATES.KILLED or res[1] == TASK_STATES.DELETED)

            res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.CLEANED)

    # def test_killed(self):
    #     num_add = 10
    #     ids = []
    #     priors = []
    #     for i in range(num_add):
    #         priors.append(random.uniform(0, 1))
    #         ids.append(self._db.add('echo "%d"' % i, priority=priors[-1]))
    #
    #     time.sleep(self._delay)
    #
    #     for i in range(num_add):
    #         tsk, id = self._db.checkout()
    #
    #         res = self._db.query("select task_id, state from tasks where task_id = '%s'" % id)[0]
    #         self.assertTrue(res[0] == id)
    #         self.assertTrue(res[1] == TASK_STATES.RUNNING)
    #
    #         res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
    #                              % id)[0]
    #         self.assertTrue(res[0] == id)
    #         self.assertTrue(res[1] == TASK_LOG_ACTIONS.RAN or res[1] == TASK_LOG_ACTIONS.RAN_FROM_CHECKPOINT)
    #
    #         time.sleep(self._delay)
    #
    #     conf = {}
    #     conf.update(self._conf)
    #     conf['intent'] = 'run'
    #     db2 = SQLiteTaskDB(**conf)
    #     kids = db2.query("SELECT TASK_ID FROM TASKS WHERE STATE = '%s'" % TASK_STATES.KILLED)
    #     kids = [kid[0] for kid in kids]
    #     self.assertTrue(sorted(kids) == sorted(ids))
    #     db2.close()
    #
    #     with SQLiteTaskDB(**conf) as db2:
    #         kids = db2.query("SELECT TASK_ID FROM TASKS WHERE STATE = '%s'" % TASK_STATES.KILLED)
    #         kids = [kid[0] for kid in kids]
    #         self.assertTrue(sorted(kids) == sorted(ids))

    def test_checkout_checkin(self):
        num_add = 10
        ids = []
        priors = []
        for i in range(num_add):
            priors.append(random.uniform(0, 1))
            ids.append(self._db.add('echo "%d"' % i, priority=priors[-1]))

        time.sleep(self._delay)

        for i in range(num_add):
            tsk, id = self._db.checkout()

            res = self._db.query("select task_id, state from tasks where task_id = '%s'" % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_STATES.RUNNING)

            res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.RAN or res[1] == TASK_LOG_ACTIONS.RAN_FROM_CHECKPOINT)

            time.sleep(self._delay)

            self._db.checkin(id, VALID_LOG_CHECKIN_ACTIONS[i % len(VALID_LOG_CHECKIN_ACTIONS)])

            res = self._db.query("select task_id, state from tasks where task_id = '%s'" % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == VALID_LOG_CHECKIN_ACTIONS[i % len(VALID_LOG_CHECKIN_ACTIONS)])

            res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == VALID_LOG_CHECKIN_ACTIONS[i % len(VALID_LOG_CHECKIN_ACTIONS)])

            time.sleep(self._delay)

    def test_checkout_checkin_state(self):
        num_add = 10
        ids = []
        priors = []
        for i in range(num_add):
            priors.append(random.uniform(0, 1))
            ids.append(self._db.add('echo "%d"' % i, priority=priors[-1]))

        time.sleep(self._delay)

        for i in range(num_add):
            tsk, id = self._db.checkout()

            res = self._db.query("select task_id, state from tasks where task_id = '%s'" % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_STATES.RUNNING)

            res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_LOG_ACTIONS.RAN or res[1] == TASK_LOG_ACTIONS.RAN_FROM_CHECKPOINT)

            time.sleep(self._delay)

            self._db.checkin(id, TASK_STATES.FAILED)

            res = self._db.query("select task_id, state from tasks where task_id = '%s'" % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_STATES.FAILED)

            res = self._db.query("select task_id, action, time from logs where task_id = '%s' ORDER BY log_id DESC"
                                 % id)[0]
            self.assertTrue(res[0] == id)
            self.assertTrue(res[1] == TASK_STATES.FAILED)

            time.sleep(self._delay)

        cids = []
        for i in range(num_add):
            tsk, id = self._db.checkout(state=TASK_STATES.FAILED)
            cids.append(id)

        self.assertTrue(sorted(cids) == sorted(ids))
