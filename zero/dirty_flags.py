from uuid import uuid4
import os
from .path_converter import PathConverter
from .globals import ANTI_COLLISION_HASH

# TODO: Wrap the mutually exclusing and CLEAN, DIRTY and REMOTE flags in state machine.
# The following transitions are possible:
# DIRTY -> CLEAN
# CLEAN -> REMOTE
# REMOTE -> CLEAN
# CLEAN -> DIRTY


APPENDIX = "_DIRTY"


class DirtyFlags:

    def __init__(self, cache_folder):
        self.path_converter = PathConverter(cache_folder)

    @staticmethod
    def generate_uuid():
        return str(uuid4())

    def has_dirty_flag(self, path):
        return os.path.isfile(self._dirty_flag_path_from_fuse_path(path))

    def set_dirty_flag(self, path):
        with open(self._dirty_flag_path_from_fuse_path(path), "w"):
            pass

    def remove_dirty_flag(self, path):
        os.remove(self._dirty_flag_path_from_fuse_path(path))

    def _dirty_flag_path_from_fuse_path(self, path):
        cache_path = self.path_converter.to_cache_path(path)
        return cache_path + ANTI_COLLISION_HASH + APPENDIX
