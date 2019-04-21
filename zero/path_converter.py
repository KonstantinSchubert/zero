ANTI_COLLISION_HASH = ""


class PathConverter:

    def __init__(self, cache_folder):
        self.cache_folder = cache_folder
        self.anti_collision_hash = ANTI_COLLISION_HASH

    def to_cache_path(self, fuse_path):
        """Returns the path in the cache file system corresponding
        to a path in the fuse file system without checking if
        that node exists.
        """
        return self.cache_folder + fuse_path

    def to_fuse_path(self, cache_path):
        """ Returns the path in the fuse file system given the path in the
        cache system"""
        return cache_path.replace(self.cache_folder, "")

    def is_dummy(self, cache_path):
        """
        Returns true if the file is a dummy file
        """
        if cache_path.endswith(self.anti_collision_hash + "dummy"):
            return True

    def add_dummy_ending(self, cache_path):
        return cache_path + self.anti_collision_hash + "dummy"

    def strip_dummy_ending(self, cache_path):
        if self.is_dummy(cache_path):
            num_characters_to_strip = len(self.anti_collision_hash + "dummy")
            return cache_path[:-num_characters_to_strip]
        else:
            return cache_path
