import os

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
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    @on_cache_path_or_dummy
    def getattr(self, path, fh=None):
        stat = os.lstat(path)
        return dict((key, getattr(stat, key)) for key in ('st_atime', 'st_ctime',
             'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    @on_cache_path_or_dummy
    def chmod(self, path, mode):
        os.chmod(path, mode)

    @on_cache_path_or_dummy
    def chown(self, path, uid, gid):
        return os.chown(path, uid, gid)


    getxattr = None


    def link(self, target, source):
        raise # What if target or source are not in our file system??
    
    listxattr = None

    def mkdir(self, path, mode):
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

    # def readlink(??):
    #     todo

    def release(self, path, fh):
        # I think the file handle will be the one for the file in the cache?
        return os.close(fh)

    # def rename(self, old, new):
    #     todo

    # def statfs(self, path):
    #     todo

    # def symlink(self, target, source):
    #     todo

    # def truncate(self, path, length, fh=None):
    #     todo

    # def unlink(???):
    #     todo

    # def utimes(????):
    #     todo

    def write(self, path, data, offset, fh):
        self.cache.write(self, path, data, offset, fh)