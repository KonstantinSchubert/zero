import os
from .state_store import StateStore


class Worker:

    def __init__(self, converter, api):
        self.api = api
        self.converter = converter
        self.state_store = StateStore()

    def _clean_path(self, path):
        self.state_store.set_cleaning(path)
        with open(self.converter.to_cache_path(path)) as file_to_upload:
            self.api.upload(file_to_upload, path)
        self.state_store.set_clean(path)

    def _delete_path(self, path):
        # Todo: Obtin path lock or make operation atomic in sqlite
        self.state_store.set_deleting(path)
        self.api.delete(path)
        self.state_store.set_deleted(path)

    def replace_dummy(self, path):
        # Todo: Worry about settings permissions and timestamps
        # Todo: Worry about concurrency
        # Todo: should this function go to the Cache class and
        # instead of a worker I pass api to the cache class and an instance
        # of cache to the worker?
        cache_path = self.converter.to_cache_path(path)
        with open(cache_path, "w+b") as file:
            file.write(self.api.download(path).read())
        os.remove(self.converter.add_dummy_ending(cache_path))

    def _create_dummy(self, path):
        # Todo: Worry about settings permissions and timestamps
        # Todo: Worry about concurrency
        # Todo: should this function go to the Cache class and
        # instead of a worker I pass api to the cache class and an instance
        # of cache to the worker?
        cache_path = self.converter.to_cache_path(path)
        with open(cache_path, "r+b") as file:
            self.api.upload(file, path)
        os.remove(cache_path)
        os.open(
            self.converter.add_dummy_ending(cache_path),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        )
