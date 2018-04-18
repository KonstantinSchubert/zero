import os
from fuse import FuseOSError

CACHE_ENDINGS = [
    "dirty", 
    "cleaning", 
    "todelete", 
    "deleting",
    "dummy"
]

Aside from dummy, I should store the dirty/cleaning/todelete/deleting in some kind of database(es) istead of using 
files.

This impoves discoverability and it allows me to delte a folder while its contents are still "todelete"

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

    def _add_cache_ending(self, cache_path, ending):
        return cache_path + self.anti_collision_hash + ending

    def _strip_cache_ending(self, cache_path):
        ending = self._get_cache_ending(cache_path)
        if ending:
            num_characters_to_strip = len(self.anti_collision_hash)+len(ending)
            return cache_path[:-num_characters_to_strip]
        else:
            return cache_path

    def _get_path_or_dummy(self, fuse_path):
        """Get cache path for given fuse_path.
        If it is a file and file is not in cache, return path to dummy file.
        If there is no diummy file either, then the file does not exist.
        In this case, return None
        """
        cache_path = self._to_cache_path(fuse_path)
        if os.path.exists(cache_path):
            return cache_path
        elif os.path.exists(cache_path + self.anti_collision_hash + "dummy"):
            return cache_path + "dummy"
        return None

    def _get_path(self, fuse_path):
        cache_path = self._to_cache_path(fuse_path)
        if os.path.exists(cache_path + self.anti_collision_hash + "dummy"):
            raise # todo: implement this
            # synchronously download real file and replace dummy 
        return cache_path


    def _list_nodes_and_dummies(self, dir_path):
        all_nodes = os.listdir(dir_path)
        return (
            node for node in all_nodes 
            if self._get_cache_ending(node) in [ None, "dummy"]
        )

    def _touch_dirty_file(self, cache_path):
        # How will the cache manager find this ins a big file tree? Mark the parent dirs as "todo" somehow?
        dirty_file = cache_path + self.anti_collision_hash + "dirty"
        with open(dirty_file, 'a'):
            os.utime(dirty_file, times=None)


    def _touch_todelete_file(self, cache_path):
        # How will the cache manager find this in a big file tree? Mark the parent dirs as "todo" somehow?
        dirty_file = cache_path + self.anti_collision_hash + "todelete"
        with open(dirty_file, 'a'):
            os.utime(dirty_file, times=None)


    def list(self, cache_dir_path, fh):
        return ['.', '..'] + [
            self._strip_cache_ending(path) for path 
            in self._list_nodes_and_dummies(cache_dir_path)
        ]

    def mkdir(self, fuse_path, mode):
        cache_path = self._to_cache_path(fuse_path)
        os.mkdir(cache_path, mode)
        self._touch_dirty_file(cache_path)

    def write(self, rwlock, path, data, offset, fh):
        # I think the file handle will be the one for the file in the cache?
        with rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)
        # todo: add dirty file

    def create(self, path, mode):
        cache_path = self._to_cache_path(path)
        result = os.open(cache_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        self._touch_dirty_file(cache_path)
        return result

    def unlink(self, rwlock, cache_path):
        with rwlock:
            os.unlink(cache_path)
            # todo: also delete any potential dirty-notes for this cache path
            # path might be dummy file path, so strip ending
            cache_path = self._strip_cache_ending(cache_path)
            self._touch_todelete_file(cache_path)


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


