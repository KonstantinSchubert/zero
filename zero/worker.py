import logging
import time
import subprocess
from multiprocessing import Process
from .locking import NodeLockedException, PathLock
from .remote_identifiers import RemoteIdentifiers
from .events import EventListener, FileDeleteEvent, FileUpdateOrCreateEvent
from .dirty_flags import DirtyFlags
from .states import StateMachine

logger = logging.getLogger("spam_application")


class UploadAbortException(Exception):
    pass


def upload(api, file_to_upload, new_uuid):
    # Maybe I can inline this helper method
    # exactly where it is used?
    api.upload(file=file_to_upload, file_uuid=new_uuid)


class Worker:

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

    # def run(self):
    #     self.clean()
    #     # self.purge()
    #     self.order_cache()

    def run_delete_watcher(self):
        with EventListener(FileDeleteEvent.topic) as deletion_listener:
            while True:
                time.sleep(1)
                for message in deletion_listener.yield_events():
                    uuid = message["uuid"]
                    # TODO: the message must contain the uuid of the file to be deleted already,
                    # since the path may no longer exist.
                    if uuid is not None:
                        self.api.delete(uuid)

    def run_clean_watcher(self):
        with EventListener(FileUpdateOrCreateEvent.topic) as cleaning_listener:
            while True:
                time.sleep(1)
                for message in cleaning_listener.yield_events():
                    path = message["path"]
                    print("Received message to clean " + path)
                    with PathLock(
                        path,
                        self.inode_store,
                        acquisition_max_retries=10,
                        high_priority=False,
                    ) as lock:
                        # - check if file exists and is still dirty
                        if not self.dirty_flags.has_dirty_flag(path):
                            "No dirty flag found on path, aborting. This can happen"
                        # - get old uuid if it exists
                        old_uuid = self.remote_identifiers.get_uuid_or_none(
                            path
                        )
                        if old_uuid:
                            # - If yes, delete old version of file on remote
                            self.api.delete(old_uuid)

                        try:
                            new_uuid = self.upload_file(path=path, lock=lock)
                            self.states.dirty_to_clean(path)
                            self.remote_identifiers.set_uuid(
                                path=path, uuid=new_uuid
                            )
                        except UploadAbortException:
                            print(f"upload of {path} was ABORTED")

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

    def upload_file(self, path, lock):
        new_uuid = RemoteIdentifiers.generate_uuid()
        with open(self.converter.to_cache_path(path), "rb") as file_to_upload:
            print(f"cleaning {path}")
            # since I don't want to mess with the b2 library code
            # but I do want to be able to interrupt the upload
            # the best option seems to be using python multiprocessing
            # https://docs.python.org/3/library/multiprocessing.html#the-process-class
            upload_process = Process(
                target=upload, args=(self.api, file_to_upload, new_uuid)
            )
            upload_process.start()
            while upload_process.is_alive():
                print(f"upload of {path} is alive")
                time.sleep(0.1)
                if lock.abort_requested():
                    upload_process.terminate()
                    raise UploadAbortException
        return new_uuid

    # def evict(self, number_of_files):
    #     """Remove unneeded files from cache"""
    #     # To decide which files to evict,
    #     # join state table with rank table
    #     # and look at files with low rank who are CLEAN
    #     evictees = self.ranker.get_eviction_candidates(number_of_files)
    #     print(evictees)
    #     for inode in evictees:
    #         self.cache.create_dummy(inode)

    # def prime(self, number_of_files):
    #     """Fill the cache with files from remote
    #     that are predicted to be needed.
    #     """
    #     # To decide which files to prime with,
    #     # join state table with rank table and look at files with high
    #     # rank who are REMOTE
    #     primees = self.ranker.get_priming_candidates(number_of_files)
    #     for inode in primees:
    #         self.cache.replace_dummy(inode)

    # def order_cache(self):
    #     # TODO: Make sure that biggest file < 0.1 * target_disk_usage, else this won't work.
    #     if (
    #         abs(self.get_disk_usage() - self.target_disk_usage)
    #         < 1.2 * self.get_size_of_biggest_file()
    #         and self.ranker.is_sufficiently_sorted()
    #     ):
    #         print(
    #             f"""Cache has the right size and is filled with the right files.
    #             Current disk usage {self.get_disk_usage()}
    #             Target disk usage {self.target_disk_usage}
    #             Tolerance {1.2 * self.get_size_of_biggest_file()}
    #             """
    #         )
    #         return
    #     elif self.get_disk_usage() > self.target_disk_usage:
    #         # If I want to evict and prime with a higher number
    #         # of files then I need to make sure I don't overshoot,
    #         # so I have to get slower as I approach the boundary
    #         # or increase the tolerance to a higher multiple of the
    #         # biggest file
    #         print("Evicting")
    #         self.evict(1)
    #     else:
    #         print("Priming")
    #         self.prime(1)
