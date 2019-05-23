import logging
import time
import subprocess
from multiprocessing import Process
from .locking import NodeLockedException, PathLock, NodeLock
from .remote_identifiers import RemoteIdentifiers


logger = logging.getLogger("spam_application")


def upload(api, file_to_upload, new_uuid, uuid_of_previous_version):
    # Maybe I can inline this helper method
    # exactly where it is used?
    api.upload(
        file=file_to_upload,
        file_uuid=new_uuid,
        file_uuid_to_replace=uuid_of_previous_version,
    )


class Worker:

    def __init__(self, cache, ranker, api, target_disk_usage):
        self.api = api
        self.target_disk_usage = target_disk_usage
        # Todo: Write methods in the cache class which wrap the
        # objects from the following two objects that I am using here
        self.converter = cache.converter
        self.state_store = cache.state_store
        self.inode_store = cache.inode_store
        self.ranker = ranker
        self.cache = cache
        cache_folder = self.converter.cache_folder  # Fix this hack
        self.remote_identifiers = RemoteIdentifiers(cache_folder)

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

    def _clean_inode(self, inode):
        if not self.state_store.is_dirty(inode):
            # This can happen if the file was deleted in the meantime
            print("Cannot clean inode because inode is not DIRTY")
            return
        path = self.inode_store.get_paths(inode)[0]
        with PathLock(path, self.inode_store) as lock:
            uuid_of_previous_version = self.remote_identifiers.get_uuid_or_none(
                path
            )
            new_uuid = RemoteIdentifiers.generate_uuid()
            with open(
                self.converter.to_cache_path(path), "rb"
            ) as file_to_upload:
                print(f"cleaning {path}")
                # since I don't want to mess with the b2 library code
                # but I do want to be able to interrupt the upload
                # the best option seems to be using python multiprocessing
                # https://docs.python.org/3/library/multiprocessing.html#the-process-class
                upload_process = Process(
                    target=upload,
                    args=(
                        self.api,
                        file_to_upload,
                        new_uuid,
                        uuid_of_previous_version,
                    ),
                )
                upload_process.start()
                while upload_process.is_alive():
                    print(f"upload of {path} is alive")
                    time.sleep(0.1)
                    if lock.abort_requested():
                        upload_process.terminate()
                        print(f"upload of {path} was ABORTED")
                        return
                        # Might want to raise an exception here
                        # instead wich is caught one method above
            self.remote_identifiers.set_uuid(path=path, uuid=new_uuid)
            self.state_store.set_clean(inode)

    def _delete_inode(self, inode):
        with NodeLock(inode, exclusive=True):
            if not self.state_store.is_todelete(inode):
                # This can happen if the file was re-created in the mantime
                print("Cannot delete inode because inode is not TODELETE")
                return
            try:
                path = self.inode_store.get_paths(inode)[0]
            except IndexError:
                # TODO: In future, this function will direclty get the path,
                # so these kind of gymnastics won't need to happen
                print("Path does not exist, not deleting")
                return
            file_uuid = self.remote_identifiers.get_uuid_or_none(path)
            if file_uuid is None:
                print(
                    "Not deleting file because no file_uuid is found. "
                    "It seems that it was never uploaded."
                )
                return
            self.api.delete(file_uuid)
            self.state_store.set_deleted(inode)
            self.remote_identifiers.delete(path)

    def clean(self):
        """Uplaod dirty files to remote"""
        for inode in self.state_store.get_dirty_inodes():
            print(f"Cleaning inode {inode}")
            try:
                self._clean_inode(inode)
            except NodeLockedException:
                print(f"Could not clean: {inode} is locked")

    def purge(self):
        """Remove todelete files from remote"""

        # TODO:
        # Instead of looking at the state_store (which I will try to remove in favor of a "DIRTY" file-based flag),
        # listen to FileDeleteEvents.

        # I must introduce a unique, random identifier for every file (similar to the inode, which was unique,
        # but not random, and therefore needed to be managed via a table.)
        # This unique, random identifier then needs to be included the in the FileDeleteEvent.

        # This unique, random identifier should be used everywhere to identify a file towards the storage api.

        # It must be random and can not be a hash, because a hash is not unique. Multiple files can have the same hash,
        # concurrently or in short sequence, leading to all kinds of concurrency headaches.

        # Using a unique random identifier, we can be sure that no file in future will contain the same identifier,
        # so we can take our sweet time to tell the API to delete it.

        # Of course we can still do de-duplicating with the storage api using reference coudning.

        # We do NOT need to obtain a lock for deletion because if the file is re-created after the delete message/event
        # has been dispatched, we can rely on the fact that the file will have new random identifier.

        # OLD CODE:
        for inode in self.state_store.get_todelete_inodes():
            print(f"Deleting inode {inode}")
            try:
                self._delete_inode(inode)
            except NodeLockedException:
                print(f"Could not delete: {inode} is locked")

    def evict(self, number_of_files):
        """Remove unneeded files from cache"""
        # To decide which files to evict,
        # join state table with rank table
        # and look at files with low rank who are CLEAN
        evictees = self.ranker.get_eviction_candidates(number_of_files)
        print(evictees)
        for inode in evictees:
            self.cache.create_dummy(inode)

    def prime(self, number_of_files):
        """Fill the cache with files from remote
        that are predicted to be needed.
        """
        # To decide which files to prime with,
        # join state table with rank table and look at files with high
        # rank who are REMOTE
        primees = self.ranker.get_priming_candidates(number_of_files)
        for inode in primees:
            self.cache.replace_dummy(inode)

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

    def run(self):
        print("Running worker")
        self.clean()
        self.purge()
        self.order_cache()
