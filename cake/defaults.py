#!/usr/bin/env python

DEF_TASKDB_CONF = {'timeout': 10.0,  # seconds
                   'task_checkout_delay': 1.0,  # seconds
                   'task_checkout_num_tries': 10}


class TASK_STATES(object):
    QUEUED_NO_DEP = 'QUEUED_NO_DEP'
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    SUCCEEDED = 'SUCCEEDED'
    CHECKPOINTED = 'CHECKPOINTED'
    KILLED = 'KILLED'
    DELETED = 'DELETED'


LIST_OF_TASK_STATES = [TASK_STATES.QUEUED_NO_DEP,
                       TASK_STATES.RUNNING,
                       TASK_STATES.FAILED,
                       TASK_STATES.SUCCEEDED,
                       TASK_STATES.CHECKPOINTED,
                       TASK_STATES.DELETED,
                       TASK_STATES.KILLED]


class TASK_LOG_ACTIONS(object):
    ADDED = 'ADDED'
    RAN = 'RAN'
    RAN_FROM_CHECKPOINT = 'RAN_FROM_CHECKPOINT'
    DELETED = 'DELETED'
    RESET = 'RESET'
    FAILED = 'FAILED'
    SUCCEEDED = 'SUCCEEDED'
    CHECKPOINTED = 'CHECKPOINTED'
    KILLED = 'KILLED'
    UPDATED = 'UPDATED'
    CLEANED = 'CLEANED'


VALID_LOG_CHECKIN_ACTIONS = [TASK_LOG_ACTIONS.FAILED,
                             TASK_LOG_ACTIONS.SUCCEEDED,
                             TASK_LOG_ACTIONS.CHECKPOINTED,
                             TASK_LOG_ACTIONS.KILLED]


class TASKDB_STATES(object):
    RUNNING = 'RUNNING'
    PAUSED = 'PAUSED'
