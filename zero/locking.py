import os
import time
import portalocker
import hashlib
from .path_utils import yield_partials

LOCKDIR = "/tmp/zero-locks/"
ABORT_REQUEST_DIR = "/tmp/zero-abort-requests/"


class NodeLockedException(Exception):
    pass


def hash_string(string):
    string_hash = hashlib.new("md5")
    string_hash.update(string.encode())
    return string_hash.hexdigest()


class PathLock:

    def __init__(
        self,
        path,
        lock_creator=None,
        exclusive_lock_on_path=False,
        exclusive_lock_on_leaf=True,
        high_priority=False,
        acquisition_max_retries=0,
    ):
        """A path has the form /path/path/path/path/leaf.
        A trailing slash is ignored, the last node is always
        considerd the leaf.
        By default, a shared lock will be obtained on all nodes
        of the path except the last one and an exclusive
        lock on the last node, the "leaf" of the path.
        """
        partials = list(yield_partials(path))
        self.locks = []
        # Locks for non-leaf-partials
        for path in partials[:-1]:
            self.locks.append(
                NodeLock(
                    lock_id=hash_string(path),
                    exclusive=exclusive_lock_on_path,
                    acquisition_max_retries=acquisition_max_retries,
                    high_priority=high_priority,
                    lock_creator=lock_creator,
                )
            )
        # Lock for leaf
        path = partials[-1]
        self.locks.append(
            NodeLock(
                lock_id=hash_string(path),
                exclusive=exclusive_lock_on_leaf,
                acquisition_max_retries=acquisition_max_retries,
                high_priority=high_priority,
                lock_creator=lock_creator,
            )
        )

    def __enter__(self):
        for lock in self.locks:
            lock.__enter__()
        return self

    def __exit__(self, *args):
        for lock in self.locks:
            lock.__exit__()

    def abort_requested(self):
        for lock in self.locks:
            if lock.abort_requested():
                return True
        return False


class NodeLock:

    def __init__(
        self,
        lock_id,
        exclusive,
        lock_creator,
        acquisition_max_retries=0,
        high_priority=False,
    ):
        self.exclusive = exclusive
        self.acquisition_max_retries = acquisition_max_retries
        self.lock_id = lock_id
        self.high_priority = high_priority
        self.lock_creator = lock_creator or "Unknown"

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
        raise NodeLockedException

    def __exit__(self, *args):
        self._unlock()
        # print(f"unlocked {self.lock_id}")

    def _get_abort_request_file_name(self):
        return f"{ABORT_REQUEST_DIR}{self.lock_id}"

    def abort_requested(self):
        return os.path.exists(self._get_abort_request_file_name())

    def _try_locking(self):
        if not os.path.exists(LOCKDIR):
            os.mkdir(LOCKDIR)
        # print(f"try locking {self.lock_id}")
        try:
            # portalocker.Lock has its own retry functionality,
            # But we cannot use it here, because we want to be able
            # to "request_abort".
            # It's a bit of a layered approach to build a high-level api
            # on top of another high-level api such as portalocker.Lock.
            # But since things are still evolving around here, it will leave it as is.
            self.lock = portalocker.Lock(
                filename=LOCKDIR + str(self.lock_id),
                fail_when_locked=True,
                flags=self._get_flags(),
            )
            self.lock.acquire()
        except portalocker.exceptions.AlreadyLocked:
            # print("Failed to lock")
            if self.high_priority:
                self._request_abort()
            return False
        # print(f"Managed to lock {self.lock_id}")
        self._remove_abort_request()
        return True

    def _get_flags(self):
        if self.exclusive:
            return portalocker.LOCK_NB | portalocker.LOCK_EX
        else:
            return portalocker.LOCK_NB | portalocker.LOCK_SH

    def _unlock(self):
        self.lock.release()

    def _remove_abort_request(self):
        if self.abort_requested():
            os.remove(self._get_abort_request_file_name())

    def _request_abort(self):
        print(f"{self.lock_creator} is requesting abort")
        if not os.path.exists(ABORT_REQUEST_DIR):
            os.mkdir(ABORT_REQUEST_DIR)
        open(self._get_abort_request_file_name(), "w").close()
