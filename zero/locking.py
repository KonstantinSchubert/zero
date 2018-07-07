import sqlite3
import time


class InodeLockedException(Exception):
    pass


class InodeLock:
    # TODO: Use a memory-based locking solution to make sure that the
    # locks disappear when the computer is shut down?
    # TODO2: Distinguish between read locks and write locks.
    # Multiple read locks at the same time are allowed.
    # But if a write lock exists, there can be no other write lock and no other read lock.
    def __init__(
        self, inode, cache, acquisition_max_retries=0, high_priority=False
    ):
        self.cache = cache
        self.acquisition_max_retries = acquisition_max_retries
        self.inode = inode
        self.high_priority = high_priority
        print("inode in init", self.inode)
        self.connection = sqlite3.connect("lock_db.sqlite3", timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS locks (inode integer primary key, abort_requested integer)"""
            )

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
        with self.connection:
            self.connection.execute("""BEGIN IMMEDIATE""")
            assert self._is_locked()
            self._unlock()
            print(f"unlocked {self.inode}")

    def abort_requested(self):
        cursor = self.connection.execute(
            """SELECT * FROM locks WHERE inode = ? AND abort_requested = 1 """,
            (self.inode,),
        )
        return cursor.fetchone() is not None

    def _file_is_open(self):
        if self.cache.state_store.is_remote(self.inode):
            return False
        paths = self.cache.inode_store.get_paths(self.inode)
        # for now simply check ths first one
        if paths == []:
            return False
        path = paths[0]
        cache_path = self.cache.converter.to_cache_path(path)
        try:
            with open(cache_path, "r+"):
                pass
        except IOError:
            return True
        return False

    def _try_locking(self):
        print(f"TRY LOCKING {self.inode}")

        # Check if file is open
        # Path can not be locked if file is open,
        # because this means that the OS is writing to it
        if self._file_is_open():
            print("Cannot lock inode if file is open")
            return False

        with self.connection:
            # We must disallow any other process from even reading the
            # lock situation before we read and then change the lock situation
            self.connection.execute("""BEGIN IMMEDIATE""")
            if not self._is_locked():
                self._lock()
                print(f"locked {self.inode}")
                return True
            elif self.high_priority:
                self._request_abort()
            return False

    def _is_locked(self):
        cursor = self.connection.execute(
            """SELECT * FROM locks WHERE inode = ? """, (self.inode,)
        )
        return cursor.fetchone() is not None

    def _lock(self):
        self.connection.execute(
            """INSERT INTO locks (inode) VALUES (?)""", (self.inode,)
        )

    def _unlock(self):
        self.connection.execute(
            """DELETE FROM locks WHERE inode = ?""", (self.inode,)
        )

    def _request_abort(self):
        self.connection.execute(
            """UPDATE locks SET abort_requested=1 WHERE inode = ?""",
            (self.inode,),
        )
