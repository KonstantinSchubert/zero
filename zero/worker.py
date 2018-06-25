import logging
import os
from .state_store import InodeLockedException

logger = logging.getLogger("spam_application")


class Worker:

    def __init__(self, cache, api):
        self.api = api
        # Todo: Write methods in the cache class which wrap the
        # objects from the following two objects that I am using here
        self.converter = cache.converter
        self.state_store = cache.state_store
        self.inode_store = cache.inode_store
        self.ranker = cache.ranker

    def _clean_inode(self, inode):
        with self.state_store.Lock(self.state_store, inode):
            path = self.inode_store.get_paths(inode)[0]
            with open(
                self.converter.to_cache_path(path), "rb"
            ) as file_to_upload:
                self.api.upload(file_to_upload, inode)
            self.state_store.set_clean(inode)

    def _delete_inode(self, inode):
        # Todo: Obtain inode lock or make operation atomic in sqlite
        with self.state_store.Lock(self.state_store, inode):
            self.api.delete(inode)
            self.state_store.set_deleted(inode)

    def _replace_dummy(self, inode):
        # Todo: Worry about settings permissions and timestamps
        # Todo: Worry about concurrency
        # Todo: should this function go to the Cache class and
        # instead of a worker I pass api to the cache class and an instance
        # of cache to the worker?
        # TODO: Do we need a self.state_store.seet_downlaoding()?
        path = self.inode_store.get_paths(inode)[0]
        cache_path = self.converter.to_cache_path(path)
        with open(cache_path, "w+b") as file:
            file.write(self.api.download(inode).read())
        os.remove(self.converter.add_dummy_ending(cache_path))
        self.state_store.set_downloaded(inode)

    def _create_dummy(self, inode):
        # Todo: Worry about settings permissions and timestamps
        # Todo: Worry about concurrency
        # Todo: should this function go to the Cache class and
        # instead of a worker I pass api to the cache class and an instance
        # of cache to the worker?
        # TODO: Do we need a self.state_store.set_uploading()?

        # INSTEAD OF UPLOADING, MAKE SURE inode IS CLEAN?
        path = self.inode_store.get_paths(inode)[0]
        cache_path = self.converter.to_cache_path(path)
        with open(cache_path, "r+b") as file:
            self.api.upload(file, inode)
        os.remove(cache_path)
        os.open(
            self.converter.add_dummy_ending(cache_path),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        )
        self.state_store.set_remote(inode)

    def clean(self):
        """Uplaod dirty files to remote"""
        for inode in self.state_store.get_dirty_inodes():
            print(f"Cleaning inode {inode}")
            try:
                self._clean_inode(inode)
            except InodeLockedException:
                print("Could not clean: {inode} is locked")

    def purge(self):
        """Remove todelete files from remote"""
        for inode in self.state_store.get_todelete_inodes():
            print(f"Deleting inode {inode}")
            try:
                self._delete_inode(inode)
            except InodeLockedException:
                print("Could not delete: {inode} is locked")

    def evict(self):
        """Remove unneeded files from cache"""
        # To decide which files to evict,
        # join state table with rank table
        # and look at files with low rank who are states.CLEAN
        evictees = self.ranker.get_eviction_candidates(2)
        print(evictees)
        # TODO EVICT

    def prime(self):
        """Fill the cache with files from remote
        that are predicted to be needed.
        """
        # To decide which files to prime with,
        # join state table with rank table and look at files with high
        # rank who are states.REMOTE

    def run(self):
        print("Running worker")
        self.clean()
        self.purge()
        self.evict()
        self.prime()
