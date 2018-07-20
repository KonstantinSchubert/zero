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

        with self.assertRaises(AssertionError):
            lock = InodeLock(1)
            lock._unlock()

    def test_cannot_lock_locked_inode(self):
        from zero.locking import InodeLock, InodeLockedException

        lock = InodeLock(1)
        lock._lock()
        lock2 = InodeLock(1)
        with self.assertRaises(InodeLockedException):
            lock2._lock()
