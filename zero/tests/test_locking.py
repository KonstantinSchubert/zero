import unittest
import shutil


class LockTest(unittest.TestCase):

    def setUp(self):
        from zero import locking

        try:
            shutil.rmtree(locking.LOCKDIR)
        except FileNotFoundError:
            pass
        try:
            shutil.rmtree(locking.ABORT_REQUEST_DIR)
        except FileNotFoundError:
            pass

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
