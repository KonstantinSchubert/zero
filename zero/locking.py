import sqlite3
import time

LOCK_FILE = "lock_db.sqlite3"


class InodeLockedException(Exception):
    pass


connection = sqlite3.connect(LOCK_FILE, timeout=5)
with connection:
    connection.execute(
        """CREATE TABLE IF NOT EXISTS locks (inode integer primary key, abort_requested integer)"""
    )


class InodeLock:
    # TODO: Use a memory-based locking solution to make sure that the
    # locks disappear when the computer is shut down?
    # TODO2: Distinguish between read locks and write locks.
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
        with connection:
            connection.execute("""BEGIN IMMEDIATE""")
            self._unlock()
            print(f"unlocked {self.inode}")

    def abort_requested(self):
        cursor = connection.execute(
            """SELECT * FROM locks WHERE inode = ? AND abort_requested = 1 """,
            (self.inode,),
        )
        return cursor.fetchone() is not None

    def _try_locking(self):
        print(f"TRY LOCKING {self.inode}")

        with connection:
            # We must disallow any other process from even reading the
            # lock situation before we read and then change the lock situation
            connection.execute("""BEGIN IMMEDIATE""")
            try:
                self._lock()
            except InodeLockedException:
                if self.high_priority:
                    self._request_abort()
                return False
            return True

    def _lock(self):
        try:
            connection.execute(
                """INSERT INTO locks (inode) VALUES (?)""", (self.inode,)
            )
        except sqlite3.IntegrityError as e:
            if str(e) == "UNIQUE constraint failed: locks.inode":
                raise InodeLockedException
            else:
                # It was another exception, thus re-raise it
                raise
        print(f"locked {self.inode}")

    def _unlock(self):
        cursor = connection.execute(
            """DELETE FROM locks WHERE inode = ?""", (self.inode,)
        )
        assert cursor.rowcount == 1

    def _request_abort(self):
        connection.execute(
            """UPDATE locks SET abort_requested=1 WHERE inode = ?""",
            (self.inode,),
        )
