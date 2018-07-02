import os

import errno
from fuse import FuseOSError, Operations

from .cache import (
    on_cache_path,
    on_cache_path_or_dummy,
    on_cache_path_enforce_local,
)


class Filesystem(Operations):
    """Implements the fuse operations.
    Operations use cache decorators if possible and are deferred to the cache
    module if more complex cache operations are needed.
    This is a bit of a fuzzy boundary that I am not really happy with.
    """

    def __init__(self, cache):
        self.cache = cache

    @on_cache_path_or_dummy
    def access(self, path, mode):
        if path is None:
            raise FuseOSError(errno.EACCES)
        if not os.access(path, mode):
            raise FuseOSError(errno.EACCES)

    @on_cache_path_or_dummy
    def getattr(self, path, fh=None):
        if path is None:
            raise FuseOSError(errno.ENOENT)
        stat = os.lstat(path)
        vals = dict(
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
        return vals

    @on_cache_path_or_dummy
    def chmod(self, path, mode):
        return os.chmod(path, mode)

    @on_cache_path_or_dummy
    def chown(self, path, uid, gid):
        return os.chown(path, uid, gid)

    getxattr = None

    def link(self, target, source):
        print("link")
        # I can maintain a hard link table that basically lists for each path
        # the  "inode" number ( doesn't need to be the real inode number )
        # Then, whenever I replace a file with its dummy, I must consider this
        # table to also configure all other hard links to that inode. Maybe on
        # the remote end I can even use "inode" number as keys for the files
        # that are uploaded. This does not solve hard links which point from
        # my file system to another file system or vice versa
        raise NotImplementedError

    listxattr = None

    @on_cache_path_enforce_local
    def open(self, path, flags):
        return os.open(path, flags)

    def read(self, path, size, offset, fh):
        print("read", path)
        return self.cache.read(path, size, offset, fh)

    @on_cache_path_enforce_local
    def readdir(self, path, fh):
        return self.cache.list(path, fh)

    def release(self, path, fh):
        print("release", path, fh)
        # I think the file handle will be the one for the file in the cache?
        return os.close(fh)

    def flush(self, path, fh):
        print("flush", path, fh)
        # I must wait with uploading a written file until the flush and fsync
        # for it happened, right?
        # Or am I safe if I just upload *closed* files?
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        # I must wait with uploading a written file until the flush and fsync
        # for it happened, right?
        # Or am I safe if I just upload *closed* files?
        print("fsync", path, fh)
        if datasync != 0:
            return os.fdatasync(fh)
        else:
            return os.fsync(fh)

    def create(self, path, mode):
        print("create", path, mode)
        return self.cache.create(path, mode)

    def rename(self, old, new):
        print(f"called rename {old} -> {new}")
        return self.cache.rename(old, new)
        raise NotImplementedError

    @on_cache_path_or_dummy
    def statfs(self, path):
        cache_stat_info = os.statvfs(path)
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

        # Todo: Remove some keys from above's list and set them here
        # Deal with the case that that path is a dummy and the case
        # that it is not
        # stat_info[""]

        return stat_info

    @on_cache_path  # links are always local
    def readlink(self, path):
        return os.readlink(path)

    @on_cache_path
    def symlink(self, path, target):
        return os.symlink(target, path)

    @on_cache_path_enforce_local
    def truncate(self, path, length, fh=None):
        with open(path, "r+") as f:
            f.truncate(length)

    @on_cache_path
    def mkdir(self, path, mode):
        print("mkdir", path, mode)
        return os.mkdir(path, mode)

    @on_cache_path
    def rmdir(self, path, *args, **kwargs):
        print("rmdir", args, kwargs)
        return os.rmdir(path, *args, **kwargs)

    @on_cache_path_or_dummy
    def unlink(self, path):
        print("unlink")
        return self.cache.unlink(path)

    def utimes(self, **kwargs):
        raise NotImplementedError

    def write(self, path, data, offset, fh):
        print("write", path, offset, fh)
        return self.cache.write(path, data, offset, fh)
