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
        from zero.locking import NodeLock

        with self.assertRaises(Exception):
            lock = NodeLock(1)
            lock._unlock()

    def test_cannot_lock_with_exclusive_lock_if_shared_lock_exists(self):
        from zero.locking import NodeLock, NodeLockedException

        with NodeLock(1, exclusive=False):
            lock2 = NodeLock(1, exclusive=True)
            with self.assertRaises(NodeLockedException):
                lock2.__enter__()

    def test_cannot_lock_with_shared_lock_if_shared_exclusive_lock_exists(self):
        from zero.locking import NodeLock, NodeLockedException

        with NodeLock(1, exclusive=True):
            lock2 = NodeLock(1, exclusive=False)
            with self.assertRaises(NodeLockedException):
                lock2.__enter__()

    def test_can_lock_with_shard_lock_if_shared_lock_exists(self):
        from zero.locking import NodeLock

        with NodeLock(1, exclusive=False):
            lock2 = NodeLock(1, exclusive=False)
            lock2.__enter__()
