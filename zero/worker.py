import logging
import os
import time
from .locking import InodeLockedException, InodeLock
import subprocess
from multiprocessing import Process


logger = logging.getLogger("spam_application")


TARGET_DISK_USAGE = 0.1  # GB


def upload(api, file_to_upload, inode):
    # Maybe I can inline this helper method
    # exactly where it is used?
    api.upload(file_to_upload, inode)


class Worker:

    def __init__(self, cache, api):
        self.api = api
        # Todo: Write methods in the cache class which wrap the
        # objects from the following two objects that I am using here
        self.converter = cache.converter
        self.state_store = cache.state_store
        self.inode_store = cache.inode_store
        self.ranker = cache.ranker

    def get_size_of_biggest_file(self):
        """In GB"""
        # TODO: Implement this. Will need to keep track of this via a table
        return 0.01

    def get_disk_usage(self):
        """Returns cache disk use in GB"""
        path = self.converter.to_cache_path("/")
        du_output = (
            subprocess.check_output(["du", "-s", path])
            .split()[0]
            .decode("utf-8")
        )
        return float(du_output) / (1000 * 1000)

    def _clean_inode(self, inode):
        with InodeLock(inode) as lock:
            path = self.inode_store.get_paths(inode)[0]
            with open(
                self.converter.to_cache_path(path), "rb"
            ) as file_to_upload:
                print(f"cleaning {path}")
                # since I don't want to mess with the b2 library code
                # but I do want to be able to interrupt the upload
                # the best option seems to be using python multiprocessing
                # https://docs.python.org/3/library/multiprocessing.html#the-process-class
                upload_process = Process(
                    target=upload, args=(self.api, file_to_upload, inode)
                )
                upload_process.start()
                while upload_process.is_alive():
                    print(f"upload of {path} is alive")
                    time.sleep(0.1)
                    if lock.abort_requested():
                        upload_process.terminate()
                        print(f"upload of {path} was killed")
                        return
                        # Might want to raise an exception here
                        # instead wich is caught one mehtod above
            self.state_store.set_clean(inode)

    def _delete_inode(self, inode):
        # Todo: Obtain inode lock or make operation atomic in sqlite
        with InodeLock(inode):
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
                print(f"Could not clean: {inode} is locked")

    def purge(self):
        """Remove todelete files from remote"""
        for inode in self.state_store.get_todelete_inodes():
            print(f"Deleting inode {inode}")
            try:
                self._delete_inode(inode)
            except InodeLockedException:
                print(f"Could not delete: {inode} is locked")

    def evict(self, number_of_files):
        """Remove unneeded files from cache"""
        # To decide which files to evict,
        # join state table with rank table
        # and look at files with low rank who are states.CLEAN
        evictees = self.ranker.get_eviction_candidates(number_of_files)
        print(evictees)
        # TODO: EVICT

    def prime(self, number_of_files):
        """Fill the cache with files from remote
        that are predicted to be needed.
        """
        # To decide which files to prime with,
        # join state table with rank table and look at files with high
        # rank who are states.REMOTE
        primees = self.ranker.get_priming_candidates(number_of_files)
        print(primees)
        # TODO: Prime

    def order_cache(self):
        # TODO: Make sure that biggest file < 0.1 * target_disk_usage, else this won't work.
        if (
            abs(self.get_disk_usage() - TARGET_DISK_USAGE)
            < 3 * self.get_size_of_biggest_file()
            and self.ranker.is_sufficiently_sorted()
        ):
            print(
                "Cache has the right size and is filled with the right files."
            )
            return
        elif self.get_disk_usage() > TARGET_DISK_USAGE:
            # If I want to evice and prime with a higher number
            # of files then I need to make sure I don't overshoot,
            # so I have to get slower as I approach the boundary
            print("Evicting")
            self.evict(1)
        else:
            print("Priming")
            self.prime(1)

    def run(self):
        print("Running worker")
        self.clean()
        self.purge()
        self.order_cache()
