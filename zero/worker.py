import os
from .state_store import StateStore
from .b2_api import FileAPI


class Worker:

    def __init__(self, cache, api):
        self.api = api
        # Todo: Write methods in the cache class which wrap the
        # objects from the following two objects that I am using here
        self.converter = cache.converter
        self.state_store = cache.state_store

    def _clean_path(self, path):
        self.state_store.set_cleaning(path)
        with open(self.converter.to_cache_path(path)) as file_to_upload:
            self.api.upload(file_to_upload, path)
        self.state_store.set_clean(path)

    def _delete_path(self, path):
        # Todo: Obtain path lock or make operation atomic in sqlite
        self.state_store.set_deleting(path)
        self.api.delete(path)
        self.state_store.set_deleted(path)

    def _replace_dummy(self, path):
        # Todo: Worry about settings permissions and timestamps
        # Todo: Worry about concurrency
        # Todo: should this function go to the Cache class and
        # instead of a worker I pass api to the cache class and an instance
        # of cache to the worker?
        # TODO: Do we need a self.state_store.seet_downlaoding()?
        cache_path = self.converter.to_cache_path(path)
        with open(cache_path, "w+b") as file:
            file.write(self.api.download(path).read())
        os.remove(self.converter.add_dummy_ending(cache_path))
        self.state_store.set_downloaded(path)

    def _create_dummy(self, path):
        # Todo: Worry about settings permissions and timestamps
        # Todo: Worry about concurrency
        # Todo: should this function go to the Cache class and
        # instead of a worker I pass api to the cache class and an instance
        # of cache to the worker?
        # TODO: Do we need a self.state_store.set_uploading()?
        cache_path = self.converter.to_cache_path(path)
        with open(cache_path, "r+b") as file:
            self.api.upload(file, path)
        os.remove(cache_path)
        os.open(
            self.converter.add_dummy_ending(cache_path),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        )
        self.state_store.set_remote(path)

    def clean(self):
        """Uplaod dirty files to remote"""
        for path in self.state_store.get_dirty_paths():
            self._clean_path(path)

    def purge(self):
        """Remove todelete files from remote"""
        for path in self.state_store.get_todelete_paths():
            self._delete_path(path)

    def evict(self):
        """Remove unneeded files from cache"""
        # To decide which files to evict,
        # join state table with rank table
        # and look at files with low rank who are states.CLEAN

    def prime(self):
        """Fill the cache with files from remote
        that are predicted to be needed.
        """
        # To decide which files to prime with,
        # join state table with rank table and look at files with high
        # rank who are states.REMOTE

    def run(self):
        self.clean()
        self.purge()
        self.evict()
        self.prime()
