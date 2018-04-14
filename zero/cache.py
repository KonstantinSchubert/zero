import os
import errno
from fuse import FuseOSError

CACHE_ENDINGS = [
    "dirty", 
    "cleaning", 
    "todelete", 
    "deleting",
    "dummy"
]

class Cache:
    def __init__(self, cache_folder, anti_collision_hash = ""):
        self.cache_folder = cache_folder
        self.anti_collision_hash = anti_collision_hash

    def _to_cache_path(self, fuse_path):
        """Returns the path in the cache file system corresponding
        to a path in the fuse file system without checking if
        that node exists.
        """
        return self.cache_folder + fuse_path # this is probably all wrong. os.path.realpath or os.path.abspath might help

    def _get_cache_ending(self, cache_path):
        """
        Returns the cache ending of the specified node if
        it is a file and has an ending
        """
        for ending in CACHE_ENDINGS:
            if cache_path.endswith(self.anti_collision_hash + ending):
                return ending

    def _strip_cache_ending(self, cache_path):
        ending = self._get_cache_ending(cache_path)
        if ending:
            return cache_path[:-len(ending)]
        else:
            return cache_path

    def _get_path_or_dummy(self, fuse_path):
        cache_path = self._to_cache_path(fuse_path)
        if os.path.exists(cache_path):
            return cache_path
        elif os.path.exists(cache_path + self.anti_collision_hash + "dummy"):
            return cache_path + "dummy"
        raise FuseOSError(errno.EACCES)

    def _get_path(self, fuse_path):
        cache_path = self._to_cache_path(fuse_path)
        if os.path.exists(cache_path + self.anti_collision_hash + "dummy"):
            raise
            # synchronously download real file and replace dummy 
        return cache_path


    def _list_nodes_and_dummies(self, dir_path):
        all_nodes = os.listdir(dir_path)
        return (
            node for node in all_nodes 
            if self._get_cache_ending(node) in [ None, "dummy"]
        )

    def list(self, dir_path, fh):
        print(self, dir_path, fh)
        return ['.', '..'] + [
            self._strip_cache_ending(path) for path 
            in self._list_nodes_and_dummies(dir_path)
        ]

    def mkdir(self, fuse_path, mode):
        cache_path = self._to_cache_path(fuse_path)
        os.mkdir(cache_path, mode)
        dirty_file = cache_path + self.anti_collision_hash + "dirty"
        with open(dirty_file, 'a'):
            os.utime(dirty_file, times=None)

    def write(self, path, data, offset, fh):
    # I think the file handle will be the one for the file in the cache?
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)



def on_cache_path_or_dummy(func):
    def using_cache_path_or_dummy(self, fuse_path, *args, **kwargs):
        cache_path = self.cache._get_path_or_dummy(fuse_path)
        return func(self, cache_path, *args, **kwargs)
    return using_cache_path_or_dummy



def on_cache_path_enforce_local(func):
    def using_cache_path_enforce_local(self, fuse_path, *args, **kwargs):
        cache_path = self.cache._get_path(fuse_path)
        return func(self, cache_path, *args, **kwargs)
    return using_cache_path_enforce_local


