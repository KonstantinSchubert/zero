import os
from .locking import InodeLock


class Cache:

    def __init__(self, converter, state_store, inode_store, ranker):
        self.converter = converter
        self.state_store = state_store
        self.inode_store = inode_store
        self.ranker = ranker
        # instead of passing an instance here, sending a signal to the worker process might be more robust

    def _get_path_or_dummy(self, fuse_path):
        """Get cache path for given fuse_path.
        If it is a file and file is not in cache, return path to dummy file.
        If there is no diummy file either, then the file does not exist.
        In this case, return None
        """
        cache_path = self.converter.to_cache_path(fuse_path)
        dummy_cache_path = self.converter.add_dummy_ending(cache_path)
        if os.path.exists(cache_path):
            return cache_path
        elif os.path.exists(dummy_cache_path):
            return dummy_cache_path
        return None

    def _get_path(self, fuse_path):
        from .worker import Worker
        from .b2_api import FileAPI
        from .main import get_config

        config = get_config()

        # Small composition inversion. Normal is that worker has cache.
        # This could also be solved with some kind of synchronous signal.
        cache_path = self.converter.to_cache_path(fuse_path)
        if os.path.exists(self.converter.add_dummy_ending(cache_path)):
            api = FileAPI(
                account_id=config["accountId"],
                application_key=config["applicationKey"],
                bucket_id=config["bucketId"],
            )
            Worker(self, api)._replace_dummy(fuse_path)
        return cache_path

    def _list_nodes_and_dummies(self, dir_path):
        return os.listdir(dir_path)

    def list(self, cache_dir_path, fh):
        return [".", ".."] + [
            self.converter.strip_dummy_ending(path)
            for path in self._list_nodes_and_dummies(cache_dir_path)
        ]

    # Instead of wrapping the lock around each read/write operation, should I rather wrap it around the "open" operation?

    def read(self, path, size, offset, fh):
        inode = self.inode_store.get_inode(path)
        self.ranker.handle_inode_access(inode)
        with InodeLock(inode, acquisition_max_retries=100, high_priority=True):
            # TODO: Need to lock before the "assert is downloaded"
            # wrapper because else worker might evict file before locking
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def write(self, path, data, offset, fh):
        # I think the file handle will be the one for the file in the cache?
        inode = self.inode_store.get_inode(path)
        with InodeLock(inode, acquisition_max_retries=100, high_priority=True):
            os.lseek(fh, offset, 0)
            result = os.write(fh, data)
            self.state_store.set_dirty(inode)
        self.ranker.handle_inode_access(inode)
        return result

    def create(self, path, mode):
        cache_path = self.converter.to_cache_path(path)
        result = os.open(
            cache_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode
        )
        inode = self.inode_store.create_and_get_inode(path)
        self.state_store.set_dirty(inode)
        self.ranker.handle_inode_access(inode)
        return result

    def rename(self, old_path, new_path):
        self.inode_store.change_path(old_path, new_path)
        return os.rename(
            self.converter.to_cache_path(old_path),
            self.converter.to_cache_path(new_path),
        )

    def unlink(self, cache_path):
        is_link = self.is_link(cache_path)
        if is_link:
            os.unlink(cache_path)
        else:
            cache_path_stripped = self.converter.strip_dummy_ending(cache_path)
            fuse_path = self.converter.to_fuse_path(cache_path_stripped)
            inode = self.inode_store.get_inode(fuse_path)
            with InodeLock(
                inode, acquisition_max_retries=10, high_priority=True
            ):
                os.unlink(cache_path)
                self.inode_store.delete_path(fuse_path)
                # TODO: Only delete inode if no other paths are poinding to it.
                self.ranker.handle_inode_delete(inode)
                self.state_store.set_todelete(inode)

    @staticmethod
    def is_link(cache_path):
        return os.path.islink(cache_path)


def on_cache_path_or_dummy(func):

    def using_cache_path_or_dummy(self, fuse_path, *args, **kwargs):
        print(func, fuse_path, args, kwargs)
        cache_path = self.cache._get_path_or_dummy(fuse_path)
        return func(self, cache_path, *args, **kwargs)

    return using_cache_path_or_dummy


def on_cache_path_enforce_local(func):

    def using_cache_path_enforce_local(self, fuse_path, *args, **kwargs):
        print(func, fuse_path, args, kwargs)
        cache_path = self.cache._get_path(fuse_path)
        return func(self, cache_path, *args, **kwargs)

    return using_cache_path_enforce_local


def on_cache_path(func):

    def using_cache_path(self, fuse_path, *args, **kwargs):
        print(func, fuse_path, args, kwargs)
        cache_path = self.cache.converter.to_cache_path(fuse_path)
        return func(self, cache_path, *args, **kwargs)

    return using_cache_path
