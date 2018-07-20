import os
import errno
import json
from fuse import FuseOSError
from .locking import InodeLock


class Cache:

    def __init__(self, converter, state_store, inode_store, ranker, api):
        self.converter = converter
        self.state_store = state_store
        self.inode_store = inode_store
        self.ranker = ranker
        self.api = api
        # instead of passing an instance here, sending a signal to the worker process might be more robust

    def _get_path_or_dummy(self, fuse_path):
        """Get cache path for given fuse_path.
        If it is a file and file is not in cache, return path to dummy file.
        If there is no dummy file either, then the file does not exist.
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

        # Small composition inversion. Normal is that worker has cache.
        # This could also be solved with some kind of synchronous signal.
        cache_path = self.converter.to_cache_path(fuse_path)
        if os.path.exists(self.converter.add_dummy_ending(cache_path)):
            self._replace_dummy(self.inode_store.get_inode(fuse_path))
        return cache_path

    def _list_nodes_and_dummies(self, dir_path):
        return os.listdir(dir_path)

    def list(self, cache_dir_path, fh):
        return [".", ".."] + [
            self.converter.strip_dummy_ending(path)
            for path in self._list_nodes_and_dummies(cache_dir_path)
        ]

    def open(self, path, flags):
        print(f"CACHE: open {path}")
        with InodeLock(
            self.inode_store.get_inode(path),
            high_priority=True,
            acquisition_max_retries=100,
        ):
            cache_path = self._get_path(path)
            print(cache_path)
            return os.open(cache_path, flags)

    def read(self, path, size, offset, fh):
        print(f"CACHE: read {path}")
        with InodeLock(
            self.inode_store.get_inode(path),
            high_priority=True,
            acquisition_max_retries=100,
        ):
            inode = self.inode_store.get_inode(path)
            self.ranker.handle_inode_access(inode)
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def truncate(self, path, length):
        inode = self.inode_store.get_inode(path)
        with InodeLock(inode, high_priority=True, acquisition_max_retries=100):
            cache_path = self._get_path(path)
            self.state_store.set_dirty(inode)
            self.ranker.handle_inode_access(inode)
            with open(cache_path, "r+") as f:
                return f.truncate(length)

    def write(self, path, data, offset, fh):
        # No need to obtain lock because file is open
        inode = self.inode_store.get_inode(path)
        with InodeLock(
            self.inode_store.get_inode(path),
            high_priority=True,
            acquisition_max_retries=100,
        ):
            self.ranker.handle_inode_access(inode)
            os.lseek(fh, offset, 0)
            result = os.write(fh, data)
            self.state_store.set_dirty(inode)
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

    def getattributes(self, fuse_path):
        cache_path = self._get_path_or_dummy(fuse_path)
        if cache_path is None:
            raise FuseOSError(errno.ENOENT)
        if self.converter.is_dummy(cache_path):
            with open(cache_path, "r") as file:
                try:
                    return json.load(file)
                except Exception as e:
                    # Need this temporariliy until all are migrated onto new scheme
                    print("Could not load stat for: ", cache_path)
        return self._get_stat(cache_path)

    def _get_stat(self, path):
        stat = os.lstat(path)
        stat_dict = dict(
            (key, getattr(stat, key))
            for key in (
                "st_atime",
                "st_ctime",
                "st_gid",
                "st_mode",
                "st_mtime",
                "st_nlink",
                "st_size",
                "st_uid",
            )
        )
        return stat_dict

    @staticmethod
    def is_link(cache_path):
        return os.path.islink(cache_path)

    def replace_dummy(self, inode):
        with InodeLock(inode):
            self._replace_dummy(inode)

    def _replace_dummy(self, inode):
        print("Replacing dummy")
        path = self.inode_store.get_paths(inode)[0]
        cache_path = self.converter.to_cache_path(path)
        dummy_path = self.converter.add_dummy_ending(cache_path)
        with open(dummy_path, "r") as dummy_file:
            stat_dict = json.load(dummy_file)
        # Rename should preserve permissions, creation time, user and group
        os.rename(dummy_path, cache_path)
        with open(cache_path, "w+b") as file:
            file.write(self.api.download(inode).read())
        # Set access time and modification time
        os.utime(cache_path, (stat_dict["st_atime"], stat_dict["st_mtime"]))
        self.state_store.set_downloaded(inode)

    def create_dummy(self, inode):
        with InodeLock(inode):
            path = self.inode_store.get_paths(inode)[0]
            cache_path = self.converter.to_cache_path(path)
            if not self.state_store.is_clean(inode):
                print(
                    "Cannot create dummy for inode because inode is not clean"
                )
                return
            stat_dict = self._get_stat(cache_path)
            dummy_path = self.converter.add_dummy_ending(cache_path)
            os.rename(cache_path, dummy_path)
            # Re-name to preserve file permissions, creation time, permissions and user id.
            with open(
                self.converter.add_dummy_ending(cache_path), "w"
            ) as dummy_file:
                json.dump(stat_dict, dummy_file)
            self.state_store.set_remote(inode)


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
