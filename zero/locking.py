import os
import sqlite3
import time
import portalocker

LOCK_FILE = "lock_db.sqlite3"
LOCKDIR = "/tmp/zero-locks/"


class InodeLockedException(Exception):
    pass


connection = sqlite3.connect(LOCK_FILE, timeout=5)
with connection:
    connection.execute(
        """CREATE TABLE IF NOT EXISTS locks (inode integer primary key, abort_requested integer)"""
    )


class InodeLock:
    # TODO: Distinguish between read locks and write locks.
    # Multiple read locks at the same time are allowed.
    # But if a write lock exists, there can be no other write lock and no other read lock.
    def __init__(self, inode, acquisition_max_retries=0, high_priority=False):
        self.acquisition_max_retries = acquisition_max_retries
        self.inode = inode
        self.high_priority = high_priority

    def __enter__(self):
        # Lock database while setting lock
        if self._try_locking():
            return self
        for counter in range(self.acquisition_max_retries):
            time.sleep(1.)
            # 1000 ms - We wait this long because a big upload might be locking
            # TODO: Reduce likelihood of huge uploads locking.
            # For example, when big files are written, the worker should avoid uploading
            # while the files is still being written. This is not a stric rule, more of a performence consideration
            if self._try_locking():
                return self
        raise InodeLockedException

    def __exit__(self, *args):
        self._unlock()
        # print(f"unlocked {self.inode}")

    def abort_requested(self):
        cursor = connection.execute(
            """SELECT * FROM locks WHERE inode = ? AND abort_requested = 1 """,
            (self.inode,),
        )
        was_requested = cursor.fetchone() is not None
        connection.execute(
            """DELETE from locks WHERE inode = ?""", (self.inode,)
        )
        return was_requested

    def _try_locking(self):
        if not os.path.exists(LOCKDIR):
            os.mkdir(LOCKDIR)
        # print(f"try locking {self.inode}")
        try:
            # portalocker.Lock has its own retry functionality,
            # But we cannot use it here, because we want to be able
            # to "request_abort".
            # It's a bit of a layered approach to build a high-level api
            # on top of another high-level api such as portalocker.Lock.
            # But since things are still evolving around here, it will leave it as is.
            self.lock = portalocker.Lock(
                filename=LOCKDIR + str(self.inode), fail_when_locked=True
            )
            self.lock.acquire()
        except portalocker.exceptions.AlreadyLocked:
            # print("Failed to lock")
            if self.high_priority:
                self._request_abort()
            return False
        # print(f"Managed to lock {self.inode}")
        return True

    def _unlock(self):
        self.lock.release()

    def _request_abort(self):
        connection.execute(
            """INSERT OR REPLACE INTO locks (inode, abort_requested) VALUES (?,1)""",
            (self.inode,),
        )
