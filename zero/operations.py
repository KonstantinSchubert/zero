import os

import errno
from fuse import FuseOSError, Operations

from .cache import on_cache_path, on_cache_path_or_dummy


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
        print(f"access on {path} with {mode}")
        # Check user permissions for a file
        if path is None:
            raise FuseOSError(errno.EACCES)
        if not os.access(path, mode):
            raise FuseOSError(errno.EACCES)

    def getattr(self, path, fh=None):
        return self.cache.getattributes(path)

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

    def open(self, path, flags):
        print("opening")
        return self.cache.open(path, flags)

    def read(self, path, size, offset, fh):
        print("read-fh:", fh)
        print("read", path)
        return self.cache.read(path, size, offset, fh)

    @on_cache_path_or_dummy
    def readdir(self, path, fh):
        return self.cache.list(path, fh)

    def release(self, path, fh):
        print("release", path, fh)
        return os.close(fh)

    def flush(self, path, fh):
        print("flush", path, fh)
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
        fh = self.cache.create(path, mode)
        print("create-fh", fh)
        return fh

    def rename(self, old, new):
        print(f"called rename {old} -> {new}")
        return self.cache.rename(old, new)

    def statfs(self, path):
        return self.cache.statfs(path)

    @on_cache_path  # links are always local
    def readlink(self, path):
        return os.readlink(path)

    @on_cache_path
    def symlink(self, path, target):
        return os.symlink(target, path)

    def truncate(self, path, length, fh=None):
        return self.cache.truncate(path, length)

    def mkdir(self, path, mode):
        print("mkdir", path, mode)
        return self.cache.mkdir(path, mode)

    def rmdir(self, path, *args, **kwargs):
        return self.cache.rmdir(path, *args, **kwargs)

    def unlink(self, path):
        print("unlink")
        return self.cache.unlink(path)

    def utimes(self, **kwargs):
        raise NotImplementedError

    def write(self, path, data, offset, fh):
        # print("write", path, offset, fh)
        return self.cache.write(path, data, offset, fh)
