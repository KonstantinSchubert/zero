import os

import errno
from fuse import FuseOSError, Operations
from threading import Lock

from .cache import on_cache_path_or_dummy, on_cache_path_enforce_local

class Filesystem(Operations):
    """Implements the fuse operations.
    Operations use cache decorators if possible and are deferred to the cache
    module if more complex cache operations are needed.
    This is a bit of a fuzzy boundary that I am not really happy with.
    """

    def __init__(self, api, cache):
        self.api = api
        self.cache = cache
        self.rwlock = Lock()

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
            (key, getattr(stat, key)) for key in (
                'st_atime', 'st_ctime', 'st_gid', 
                'st_mode', 'st_mtime', 'st_nlink', 
                'st_size', 'st_uid'
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
        raise # What if target or source are not in our file system??
    
    listxattr = None

    def mkdir(self, path, mode):
        print("mkdir", path, mode)
        return self.cache.mkdir(path, mode)

    @on_cache_path_enforce_local
    def open(self, path, flags):
        return os.open(path, flags)

    @on_cache_path_enforce_local
    def read(self, path, size, offset, fh):
        # I think the file handle will be the one for the file in the cache, right?
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    @on_cache_path_enforce_local
    def readdir(self, path, fh):
        return self.cache.list(path, fh)


    def release(self, path, fh):
        # I think the file handle will be the one for the file in the cache?
        return os.close(fh)


    def flush(self, path, fh):
        print("flush", path, fh)
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        print("fsync", path, fh)
        if datasync != 0:
            return os.fdatasync(fh)
        else:
            return os.fsync(fh)


    def create(self, path, mode):
        print("create", path, mode)
        self.cache.create(path, mode)
        
    def rename(self, old, new):
        raise

    def statfs(self, path):
        raise

    def readlink(self, *args, **kwargs):
        raise

    def symlink(self, target, source):
        raise

    def truncate(self, path, length, fh=None):
        raise

    def unlink(self, *args):
        raise

    def utimes(self, **kwargs):
        raise

    def write(self, path, data, offset, fh):
        self.cache.write(self, path, data, offset, fh)