from abc import ABCMeta, abstractmethod


class BaseTaskDB(metaclass=ABCMeta):
    @abstractmethod
    def checkout(self):
        """checkout a task for running"""
        pass

    @abstractmethod
    def checkin(self, state):
        """checkin a task that has been run"""
        pass

    @abstractmethod
    def add(self, cmd, id=None):
        """add a task to be run via cmd"""
        pass

    @abstractmethod
    def add_multiple(self, cmd, id=None):
        """add a tasks to be run via cmd"""
        pass

    @abstractmethod
    def update(self, id, task=None, priority=None):
        """update task with id"""
        pass

    @abstractmethod
    def delete(self, id):
        """delete task id"""
        pass

    @abstractmethod
    def reset(self):
        """reset all tasks to be rerun"""
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        return False

    def close(self):
        """shutdown the DB"""
        pass

    @abstractmethod
    def list(self):
        """list all tasks in the DB"""
        pass

    @abstractmethod
    def log(self, task_id):
        """get log for task id"""
        pass

    @abstractmethod
    def query(self, cmd):
        """run the query cmd on the database"""
        pass

    @abstractmethod
    def status(self):
        """get status of task DB"""
        pass

    @abstractmethod
    def pause(self):
        """set DB state to pause"""
        pass

    @abstractmethod
    def run(self):
        """set DB state to running"""
        pass

    @abstractmethod
    def state(self):
        """get DB state"""
        pass

    @abstractmethod
    def cleanup(self):
        """cleanup the task DB"""
        pass

    @abstractmethod
    def runtime(self):
        """get run time stats for tasks in the DB"""
        pass
