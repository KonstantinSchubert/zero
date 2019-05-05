import os
import errno
from fuse import FuseOSError
from .locking import PathLock


class Cache:

    def __init__(
        self, converter, state_store, inode_store, metadata_store, ranker, api
    ):
        self.converter = converter
        self.state_store = state_store
        self.inode_store = inode_store
        self.ranker = ranker
        self.api = api
        self.metadata_store = metadata_store
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
        with PathLock(
            path,
            self.inode_store,
            high_priority=True,
            acquisition_max_retries=100,
        ):
            cache_path = self._get_path(path)
            print(cache_path)
            return os.open(cache_path, flags)

    def read(self, path, size, offset, fh):
        print(f"CACHE: read {path}")
        with PathLock(
            path,
            self.inode_store,
            exclusive_lock_on_leaf=False,
            high_priority=True,
            acquisition_max_retries=100,
        ):
            inode = self.inode_store.get_inode(path)
            self.metadata_store.record_access()
            self.ranker.handle_inode_access(inode)
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def truncate(self, path, length):
        with PathLock(
            path,
            self.inode_store,
            high_priority=True,
            acquisition_max_retries=100,
        ):
            inode = self.inode_store.get_inode(path)
            self.metadata_store.record_content_modification(inode)
            cache_path = self._get_path(path)
            self.state_store.set_dirty(inode)
            self.ranker.handle_inode_access(inode)
            with open(cache_path, "r+") as f:
                return f.truncate(length)

    def write(self, path, data, offset, fh):
        with PathLock(
            path,
            self.inode_store,
            high_priority=True,
            acquisition_max_retries=100,
        ):
            inode = self.inode_store.get_inode(path)
            self.metadata_store.record_content_modification(inode)
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
        self.inode_store.create_path(path)
        inode = self.inode_store.get_inode(path)
        self.metadata_store.record_content_modification(inode)
        self.state_store.set_dirty(inode)
        self.ranker.handle_inode_access(inode)
        return result

    def rename(self, old_path, new_path):
        # TODO: This function has a bunch of
        # race conditions, especially
        # if we assume multiple fuse threads

        # We need a way to call PathLock that
        # creates a new path in the inode_store and also locks it in a race-
        # free manner.  We need this in multiple parts of the code,
        # also here.
        # I also need to update the ctime of the affected files
        with PathLock(
            old_path,
            self.inode_store,
            acquisition_max_retries=100,
            high_priority=True,
        ):
            existing_inode_at_new_path = self.inode_store.get_inode(new_path)
            if existing_inode_at_new_path:
                if self.state_store.exists(existing_inode_at_new_path):
                    # inode is a file
                    with PathLock(
                        new_path,
                        self.inode_store,
                        acquisition_max_retries=100,
                        high_priority=True,
                    ):
                        self._delete_file(new_path)
                else:
                    # inode is a folder:
                    self.rmdir(new_path)

            # Rename the cache files
            os.rename(
                self.converter.to_cache_path(old_path),
                self.converter.to_cache_path(new_path),
            )

            # Update the inode store
            self.inode_store.rename_paths(old_path, new_path)

    def mkdir(self, path, mode):
        print("mkdir", path)
        # TODO: We need a way to call PathLock that
        # creates a new path and also locks it in a race-
        # free manner.  We need this in multiple parts of the code
        # also here.
        self.inode_store.create_path(path)
        cache_path = self.converter.to_cache_path(path)
        return os.mkdir(cache_path, mode)

    def rmdir(self, fuse_path, *args, **kwargs):
        cache_path = self.converter.to_cache_path(fuse_path)
        print("rmdir", args, kwargs)
        with PathLock(
            fuse_path,
            self.inode_store,
            high_priority=True,
            acquisition_max_retries=100,
        ):
            self.inode_store.delete_path(fuse_path)
            return os.rmdir(cache_path, *args, **kwargs)

    def unlink(self, fuse_path):
        cache_path = self._get_path_or_dummy(fuse_path)
        is_link = self.is_link(cache_path)
        if is_link:
            os.unlink(cache_path)
        else:

            with PathLock(
                fuse_path,
                self.inode_store,
                acquisition_max_retries=10,
                high_priority=True,
            ):
                self._delete_file(fuse_path)

    def _delete_file(self, fuse_path):
        inode = self.inode_store.get_inode(fuse_path)
        cache_path = self._get_path_or_dummy(fuse_path)
        self.inode_store.delete_path(fuse_path)
        os.unlink(cache_path)  # May be actual file or dummy
        self.ranker.handle_inode_delete(inode)
        self.state_store.set_todelete(inode)

    def getattributes(self, fuse_path):
        cache_path = self._get_path_or_dummy(fuse_path)
        if cache_path is None:
            raise FuseOSError(errno.ENOENT)
        stat = os.lstat(cache_path)
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
        if fuse_path[-1] != "/":
            # If not a directory
            inode = self.inode_store.get_inode(fuse_path)
            stat_dict["st_atime"] = self.metadata_store.get_access_time(inode)
            stat_dict["st_mtime"] = self.metadata_store.get_modification_time(
                inode
            )
            stat_dict["st_ctime"] = self.metadata_store.get_change_time(inode)
            print(stat_dict)
        return stat_dict

    @staticmethod
    def is_link(cache_path):
        return os.path.islink(cache_path)

    def replace_dummy(self, inode):
        path = self.inode_store.get_paths(inode)[0]
        with PathLock(path, self.inode_store):
            self._replace_dummy(inode)

    def _replace_dummy(self, inode):
        print(f"Replacing dummy [{inode}]")
        if not self.state_store.is_remote(inode):
            print(
                f"Cannot replace dummy for inode {inode}"
                "because inode is not remote."
            )
        path = self.inode_store.get_paths(inode)[0]
        cache_path = self.converter.to_cache_path(path)
        dummy_path = self.converter.add_dummy_ending(cache_path)
        # Rename should already preserve permissions, creation time, user and group.
        # So we do not need to set them here.
        os.rename(dummy_path, cache_path)
        with open(cache_path, "w+b") as file:
            try:
                file.write(self.api.download(inode).read())
            except ConnectionError:
                raise FuseOSError(errno.ENETUNREACH)
        self.state_store.set_downloaded(inode)

    def create_dummy(self, inode):
        path = self.inode_store.get_paths(inode)[0]
        with PathLock(path, self.inode_store):
            # This can happen if the file was written to in the meantime
            if not self.state_store.is_clean(inode):
                print(
                    "Cannot create dummy for inode because inode is not clean"
                )
                return
            cache_path = self.converter.to_cache_path(path)
            dummy_path = self.converter.add_dummy_ending(cache_path)
            os.rename(cache_path, dummy_path)
            # Re-name to preserve file permissions, user id.
            self.state_store.set_remote(inode)

    def statfs(self, path):
        cache_path = self._get_path_or_dummy(path)
        cache_stat_info = os.statvfs(cache_path)
        stat_info = {
            key: getattr(cache_stat_info, key)
            for key in (
                "f_bavail",
                "f_bfree",
                "f_blocks",
                "f_bsize",
                "f_favail",
                "f_ffree",
                "f_files",
                "f_flag",
                "f_frsize",
                "f_namemax",
            )
        }
        inode = self.inode_store.get_inode(path)

        # Todo: Remove some keys from above's list and set them here
        # With info from the metadata store.
        # stat_info[""]

        return stat_info


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
