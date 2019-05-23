from uuid import uuid4
import json
import os
from .path_converter import PathConverter
from .globals import ANTI_COLLISION_HASH


APPENDIX = "_UUID"


class RemoteIdentifiers:

    def __init__(self, cache_folder):
        self.path_converter = PathConverter(cache_folder)

    @staticmethod
    def generate_uuid():
        return str(uuid4())

    def get_uuid_or_none(self, path):
        try:
            with open(self._uuid_path_from_fuse_path(path), "r") as uuid_file:
                return json.load(uuid_file)
        except IOError:
            # TODO: Make sure that we really only
            # catch the case where the file does not exist
            return None

    def set_uuid(self, path, uuid):
        with open(self._uuid_path_from_fuse_path(path), "w") as uuid_file:
            json.dump(uuid, uuid_file)

    def delete(self, path):
        os.remove(self._uuid_path_from_fuse_path(path))

    def _uuid_path_from_fuse_path(self, path):
        cache_path = self.path_converter.to_cache_path(path)
        return cache_path + ANTI_COLLISION_HASH + APPENDIX
