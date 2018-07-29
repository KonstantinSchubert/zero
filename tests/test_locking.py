import unittest
import os
from importlib import reload


class LockTest(unittest.TestCase):

    def setUp(self):
        from zero import locking

        os.remove(locking.LOCK_FILE)
        locking = reload(locking)

    def test_error_on_unlock_without_lock(self):
        from zero.locking import InodeLock

        with self.assertRaises(Exception):
            lock = InodeLock(1)
            lock._unlock()

    def test_cannot_lock_locked_inode(self):
        from zero.locking import InodeLock, InodeLockedException

        with InodeLock(1):
            lock2 = InodeLock(1)
            with self.assertRaises(InodeLockedException):
                lock2.__enter__()
