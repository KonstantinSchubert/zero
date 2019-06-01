import logging
import time
import subprocess
from multiprocessing import Process
from zero.locking import NodeLockedException, PathLock
from zero.remote_identifiers import RemoteIdentifiers
from zero.events import EventListener, FileDeleteEvent, FileUpdateOrCreateEvent
from zero.dirty_flags import DirtyFlags
from zero.states import StateMachine

logger = logging.getLogger("spam_application")


class Balancer:

    def __init__(self, cache, api, target_disk_usage):
        self.api = api
        self.target_disk_usage = target_disk_usage
        # Todo: Write methods in the cache class which wrap the
        # objects from the following two objects that I am using here
        self.converter = cache.converter
        self.inode_store = cache.inode_store
        # self.ranker = ranker
        self.cache = cache
        cache_folder = self.converter.cache_folder  # Fix this hack
        self.states = StateMachine(cache_folder=cache_folder)
        self.remote_identifiers = RemoteIdentifiers(cache_folder)
        self.dirty_flags = DirtyFlags(cache_folder)

    def get_size_of_biggest_file(self):
        """In GB"""
        # TODO: Implement this in a reasonably efficient way by caching file sizes
        path = self.converter.to_cache_path("/")
        command = (
            f"find {path} -type f -exec du -a {{}} + | sort -n -r | head -n 1"
        )
        response = subprocess.check_output(command, shell=True)
        try:
            du_output = response.split()[0].decode("utf-8")
        except IndexError:
            # There is no file.
            return 0
        return float(du_output) / (1000 * 1000)

    def get_disk_usage(self):
        """Returns cache disk use in GB"""
        path = self.converter.to_cache_path("/")
        du_output = (
            subprocess.check_output(["du", "-s", path])
            .split()[0]
            .decode("utf-8")
        )
        return float(du_output) / (1000 * 1000)

    def evict(self, number_of_files):
        """Remove unneeded files from cache"""
        # To decide which files to evict,
        # join state table with rank table
        # and look at files with low rank who are CLEAN
        evictees = self.ranker.get_eviction_candidates(number_of_files)
        print(evictees)
        for path in evictees:
            self.cache.create_dummy(path)

        # TODO: Handle race conditions and mistakes by the ranker that
        # cause create_dummy to fail.
        # For example the ranker might have returned a path that does no longer exit.

    def prime(self, number_of_files):
        """Fill the cache with files from remote
        that are predicted to be needed.
        """
        # To decide which files to prime with,
        # join state table with rank table and look at files with high
        # rank who are REMOTE
        primees = self.ranker.get_priming_candidates(number_of_files)
        for path in primees:
            self.cache.replace_dummy(path)

            # TODO: Handle race conditions and mistakes by the ranker that
            # cause replace_dummy to fail.
            # For example the ranker might have returned a path that does no longer exit.

    def order_cache(self):
        # TODO: Make sure that biggest file < 0.1 * target_disk_usage, else this won't work.
        if (
            abs(self.get_disk_usage() - self.target_disk_usage)
            < 1.2 * self.get_size_of_biggest_file()
            and self.ranker.is_sufficiently_sorted()
        ):
            print(
                f"""Cache has the right size and is filled with the right files.
                Current disk usage {self.get_disk_usage()}
                Target disk usage {self.target_disk_usage}
                Tolerance {1.2 * self.get_size_of_biggest_file()}
                """
            )
            return
        elif self.get_disk_usage() > self.target_disk_usage:
            # If I want to evict and prime with a higher number
            # of files then I need to make sure I don't overshoot,
            # so I have to get slower as I approach the boundary
            # or increase the tolerance to a higher multiple of the
            # biggest file
            print("Evicting")
            self.evict(1)
        else:
            print("Priming")
            self.prime(1)
