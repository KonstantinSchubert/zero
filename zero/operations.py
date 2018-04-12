import os

from fuse import FuseOSError, Operations
from threading import Lock

# let's use os.realpath to do the path -> cache_path mapping!!!!

class Filesystem(Operations):

    def __init__(self, api, cache):
        self.api = api
        self.cache = cache
        self.rwlock = Lock()


    def access(self, path, mode):
        actual_file_path = self.cache.get_path_or_dummy(path)
        if not os.access(actual_file_path, mode):
            raise FuseOSError(EACCES)

    def getattr(self, path, fh=None):
        cache_path = self.cache.get_path_or_dummy(path)
        stat = os.lstat(cache_path)
        return dict((key, getattr(stat, key)) for key in ('st_atime', 'st_ctime',
             'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def chmod(self, path, mode):
        cache_path = self.cache.get_path_or_dummy()
        os.chmod(cache_path, mode)
        return

    def chown(self, path, uid, gid):
        cache_path = self.cache.get_path_or_dummy()
        return os.chown(cache_path, uid, gid)


    getxattr = None


    def link(self, target, source):
        raise # What if target or source are not in our file system??
    
    listxattr = None

    def mkdir(self, path, mode):
        cache_path = cache.get_cache_path(path)
        return os.mkdir(cache_path, mode)

    def open(self, path, flags):
        cache_file_path = cache.get_path(path)
        return os.open(cache_file_path, flags)

    def read(self, path, size, offset, fh):
        # I think the file handle will be the one for the file in the cache, right?
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        # todo: Do I need to handle the file handle here?
        return ['.', '..'] + cache.list_effective_nodes(path)

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
        # I think the file handle will be the one for the file in the cache?
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)
