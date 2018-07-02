import sqlite3
import time


class InodeLockedException(Exception):
    pass


class InodeLock:
    # TODO: Use a memory-based locking solution to make sure that the
    # locks disappear when the computer is shut down?
    def __init__(self, inode, acquisition_max_retries=0):
        self.acquisition_max_retries = acquisition_max_retries
        self.inode = inode
        print("inode in init", self.inode)
        self.connection = sqlite3.connect("lock_db.sqlite3", timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS locks (inode integer primary key)"""
            )

    def __enter__(self):
        # Lock database while setting lock
        if self._try_locking():
            return
        for counter in range(self.acquisition_max_retries):
            time.sleep(1.)
            # 1000 ms - We wait this long because a big upload might be locking
            # TODO: Reduce likelihood of huge uploads locking.
            # For example, when big files are written, the worker should avoid uploading
            # while the files is still being written. This is not a stric rule, more of a performence consideration
            if self._try_locking():
                return
        raise InodeLockedException

    def __exit__(self, *args):
        with self.connection:
            self.connection.execute("""BEGIN IMMEDIATE""")
            assert self._is_locked(self.inode)
            self._unlock(self.inode)
            print(f"unlocked {self.inode}")

    def _try_locking(self):
        print(f"TRY LOCKING {self.inode}")
        with self.connection:
            # We must disallow any other process from even reading the
            # lock situation before we read and then change the lock situation
            self.connection.execute("""BEGIN IMMEDIATE""")
            if not self._is_locked(self.inode):
                self._lock(self.inode)
                print(f"locked {self.inode}")
                return True
            return False

    def _is_locked(self, inode):
        cursor = self.connection.execute(
            """SELECT * FROM locks WHERE inode = ? """, (inode,)
        )
        return cursor.fetchone() is not None

    def _lock(self, inode):
        self.connection.execute(
            """INSERT INTO locks (inode) VALUES (?)""", (inode,)
        )

    def _unlock(self, inode):
        self.connection.execute(
            """DELETE FROM locks WHERE inode = ?""", (inode,)
        )
