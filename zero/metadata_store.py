from datetime import datetime
import json
from .path_converter import PathConverter
from .globals import ANTI_COLLISION_HASH


class TIMES:
    ATIME = "atime"
    MTIME = "mtime"
    CTIME = "ctime"


# TODO: Add other metadata? Or is some of it stored via the dummy?:

#             "st_gid",
#             "st_mode",
#             "st_nlink",
#             "st_size",
#             "st_uid",


APPENDIX = "METADATA"


class MetaData:

    def __init__(self, cache_folder):
        self.path_converter = PathConverter(cache_folder)

    """Stores meta data about the file such as file access times."""

    # TODO: These hooks need to get called in more places.
    def record_content_modification(self, path):
        cache_path = self.path_converter.to_cache_path(path)
        self._record_change(cache_path)
        self._record_modification(cache_path)
        self._record_access(cache_path)

    def record_access(self, path):
        cache_path = self.path_converter.to_cache_path(path)
        self._record_access(cache_path)

    def _record_access(self, cache_path):
        """Sets the atime"""
        self._set_to_now(cache_path, TIMES.ATIME)

    def _record_modification(self, cache_path):
        """Sets the mtime"""
        self._set_to_now(cache_path, TIMES.MTIME)

    def _record_change(self, cache_path):
        """Sets the ctime"""
        self._set_to_now(cache_path, TIMES.CTIME)

    def get_access_time(self, path):
        cache_path = self.path_converter.to_cache_path(path)
        return self._get_time(cache_path, TIMES.ATIME)

    def get_modification_time(self, path):
        cache_path = self.path_converter.to_cache_path(path)
        return self._get_time(cache_path, TIMES.MTIME)

    def get_change_time(self, path):
        cache_path = self.path_converter.to_cache_path(path)
        return self._get_time(cache_path, TIMES.CTIME)

    def _get_time(self, cache_path, property: TIMES):
        with open(
            _metadata_cache_path_from_cache_path(cache_path), "r"
        ) as metadata_file:
            return json.load(metadata_file)[property]

    def create(self, cache_path):
        with open(
            _metadata_cache_path_from_cache_path(cache_path), "w+"
        ) as metadata_file:
            data = {
                TIMES.CTIME: datetime.now().timestamp(),
                TIMES.MTIME: datetime.now().timestamp(),
                TIMES.ATIME: datetime.now().timestamp(),
            }
            json.dump(data, metadata_file)

    def _set_to_now(self, cache_path: int, property: TIMES):
        with open(
            _metadata_cache_path_from_cache_path(cache_path), "r+"
        ) as metadata_file:
            data = json.load(metadata_file)
            data[property] = datetime.now().timestamp()


def _metadata_cache_path_from_cache_path(cache_path):
    return cache_path + ANTI_COLLISION_HASH + APPENDIX
