import os
import errno
import logging
from multiprocessing import current_process
from fuse import FuseOSError
from .locking import PathLock
from .events import (
    FileAccessEvent,
    FileDeleteEvent,
    FileUpdateOrCreateEvent,
    FileEvictedFromCacheEvent,
    FileLoadedIntoCacheEvent,
)
from .metadata_store import MetaData, _metadata_cache_path_from_cache_path
from .path_converter import PathConverter
from .remote_identifiers import RemoteIdentifiers
from .dirty_flags import DirtyFlags
from .globals import ANTI_COLLISION_HASH
from .states import StateMachine

log = logging.getLogger(current_process().name)


class PathDoesNotExistException(Exception):
    pass


def is_file_descriptor(cache_path):
    try:
        os.stat(cache_path)
    except OSError:
        return False
    else:
        return True


class Cache:

    def __init__(self, cache_folder, api, events_channel):
        self.cache_folder = cache_folder
        self.converter = PathConverter(cache_folder)
        self.api = api
        self.metadata_store = MetaData(cache_folder)
        self.states = StateMachine(cache_folder=cache_folder)
        self.remote_identifiers = RemoteIdentifiers(cache_folder)
        self.events_channel = events_channel
        # instead of passing an instance here, sending a signal to the worker process might be more robust

    def _get_path_or_dummy(self, fuse_path):
        """Get cache path for given fuse_path.
        If it is a file and file is not in cache, return path to dummy file.
        If there is no dummy file either, then the file does not exist.
        In this case, return None
        """
        cache_path = self.converter.to_cache_path(fuse_path)
        dummy_cache_path = self.converter.add_dummy_ending(cache_path)
        if os.path.exists(cache_path):
            return cache_path
        elif os.path.exists(dummy_cache_path):
            return dummy_cache_path
        return None

    def _get_path(self, fuse_path):
        if self.states.current_state_is_remote(path=fuse_path):
            # TODO: Escalate read lock to write lock here
            self._replace_dummy(fuse_path)
        return self.converter.to_cache_path(fuse_path)

    def _list_files_and_dummies(self, dir_path):
        return [
            item
            for item in os.listdir(dir_path)
            if ANTI_COLLISION_HASH not in item
        ]

    def list(self, cache_dir_path, fh):
        return [".", ".."] + [
            self.converter.strip_dummy_ending(path)
            for path in self._list_files_and_dummies(cache_dir_path)
        ]

    def open(self, path, flags):
        print(f"CACHE: open {path}")
        with PathLock(
            path,
            high_priority=True,
            acquisition_max_retries=100,
            lock_creator="CACHE OPEN",
        ):
            cache_path = self._get_path(path)
            print(cache_path)
            self.metadata_store.record_access(path=path)
            FileAccessEvent(self.events_channel).submit(path=path)
            return os.open(cache_path, flags)

    def read(self, path, size, offset, fh):
        print(f"CACHE: read {path}")
        with PathLock(
            path,
            exclusive_lock_on_leaf=False,
            high_priority=True,
            acquisition_max_retries=100,
            lock_creator="CACHE READ",
        ):
            TODO: ALTERNATIVE 1:
            # Do not use fh here, use path, because the file handle comes from "open" (?),
            # but it might be that between open and read, the file gets moved to remote and back
            # in this case, the file handle might change in the meantime (?)
            # Basically, "open" should do nothing apart from maybe logging the access,
            # and we are actually opening the file in here and also ensuring here that it is local.
            # In this case, we would probably want to ignore operations such as flush, fsync, etc.
            # This is probably the simpler approach, but we are probably giving up some options for
            # performance optimization and I am probably violating some POSIX behavior, such as
            # file-open -based locks (?), etc..
            # However, I could always implement another layer of abstraction in front of this "cache",
            # that acts like an actual cache and actually makes use of flush, and fsync etc.
            # This here might be relevant: https://stackoverflow.com/a/2340641
            # ALTERNATIVE2:
            #  we could actually open  file  in python during the "open",   but then we have to
            # manage the lock lifecycle such that it stays locked during all reads until the file is closed.
            # And if a write happens, the lock needs to be upgraded to a write lock, so we need to support
            # re-entry. We would also need to manage the lock without the context manager.
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def truncate(self, path, length):
        print("truncate")
        with PathLock(path, high_priority=True, acquisition_max_retries=100):
            self.metadata_store.record_content_modification(path=path)
            cache_path = self.converter.to_cache_path(path)
            FileUpdateOrCreateEvent(self.events_channel).submit(path=path)
            FileAccessEvent(self.events_channel).submit(path=path)
            with open(cache_path, "r+") as f:
                return f.truncate(length)

    def write(self, path, data, offset, fh):
        print("write")
        with PathLock(
            path,
            high_priority=True,
            acquisition_max_retries=100,
            lock_creator="CACHE WRITE",
        ):
            self.metadata_store.record_content_modification(path=path)
            FileAccessEvent(self.events_channel).submit(path=path)
            os.lseek(fh, offset, 0)
            result = os.write(fh, data)
            self.states.dirty_or_clean_to_dirty(path)
            FileUpdateOrCreateEvent(self.events_channel).submit(path=path)
            return result

    def create(self, path, mode):
        print("CREATING")
        cache_path = self.converter.to_cache_path(path)
        self.metadata_store.create(cache_path)
        result = os.open(
            cache_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode
        )
        self.metadata_store.record_content_modification(path=path)
        self.states.clean_to_dirty(path)
        FileUpdateOrCreateEvent(self.events_channel).submit(path=path)
        FileAccessEvent(self.events_channel).submit(path=path)
        return result

    def rename(self, old_path, new_path):
        print("rename")
        # TODO: Examine this function for race conditions
        # TODO: Check if I need to update the ctime of the affected files

        # TODO: Issue event to inform ranker about move?
        old_cache_path = self.converter.to_cache_path(old_path)
        new_cache_path = self.converter.to_cache_path(new_path)
        with PathLock(
            old_path,
            acquisition_max_retries=100,
            high_priority=True,
            lock_creator="CACHE RENAME",
        ):
            existing_file_desciptor_at_new_location = is_file_descriptor(
                new_cache_path
            )
            if existing_file_desciptor_at_new_location:
                # If something exists at the target path, we will overwrite it
                if os.path.islink(new_cache_path):
                    os.unlink(new_cache_path)
                elif os.path.isdir(new_cache_path):
                    # file_desciptor is a folder:
                    self.rmdir(new_cache_path)
                    # TODO: If the target contains files, we have to delete them and issue
                    # delete events!
                elif os.path.isfile(new_cache_path):
                    # file_desciptor is a file
                    with PathLock(
                        new_path,
                        acquisition_max_retries=100,
                        high_priority=True,
                    ):
                        self._delete_file(new_path)
                else:
                    raise NotImplementedError

            if os.path.isdir(old_cache_path):
                # Rename the cache files
                os.rename(old_cache_path, new_cache_path)
                # TODO: Issue event for ranker.
            else:
                # Rename the cache files
                os.rename(old_cache_path, new_cache_path)
                # TODO : This is hacky/ not in the right place
                os.rename(
                    _metadata_cache_path_from_cache_path(old_cache_path),
                    _metadata_cache_path_from_cache_path(new_cache_path),
                )
                print("renamed metadata")
                try:
                    os.rename(
                        DirtyFlags(
                            self.cache_folder
                        )._dirty_flag_path_from_fuse_path(old_path),
                        DirtyFlags(
                            self.cache_folder
                        )._dirty_flag_path_from_fuse_path(new_path),
                    )
                    print("renamed dirty flag")
                except FileNotFoundError:
                    # Dirty flag file does not exist
                    pass
                try:
                    os.rename(
                        RemoteIdentifiers(
                            self.cache_folder
                        )._uuid_path_from_fuse_path(old_path),
                        RemoteIdentifiers(
                            self.cache_folder
                        )._uuid_path_from_fuse_path(new_path),
                    )
                    print("renamed remote identifier file")
                except FileNotFoundError:
                    # UUID flag file does not exist
                    pass
                # TODO: self.metadata_store. -> record file move

    def mkdir(self, path, mode):
        print("mkdir", path)
        # TODO: We need a way to call PathLock that
        # creates a new path and also locks it in a race-
        # free manner.  We need this in multiple parts of the code
        # also here.
        cache_path = self.converter.to_cache_path(path)
        return os.mkdir(cache_path, mode)

    def rmdir(self, fuse_path, *args, **kwargs):
        cache_path = self.converter.to_cache_path(fuse_path)
        print("rmdir", args, kwargs)
        with PathLock(
            fuse_path,
            high_priority=True,
            acquisition_max_retries=100,
            lock_creator="CACHE RMDIR",
        ):
            # TODO: I assume this fails, and fuse expects it to fail, if the folder is not empty?
            return os.rmdir(cache_path, *args, **kwargs)

    def unlink(self, fuse_path):
        print(f"unlink {fuse_path}")
        cache_path = self._get_path_or_dummy(fuse_path)
        is_link = self.is_link(cache_path)
        if is_link:
            os.unlink(cache_path)
        else:

            with PathLock(
                fuse_path,
                acquisition_max_retries=10,
                high_priority=True,
                lock_creator="CACHE UNLINK",
            ):
                self._delete_file(fuse_path)

    def _delete_file(self, fuse_path):
        try:
            os.unlink(self._get_path_or_dummy(fuse_path))

            # Delete all flags. This is hacky and should be cleaned up
            self.metadata_store.delete(fuse_path)
            try:
                DirtyFlags(self.cache_folder).remove_dirty_flag(fuse_path)
            except Exception:
                # There may be no dirty flag
                pass
            ##

            uuid = self.remote_identifiers.get_uuid_or_none(fuse_path)
            if uuid:
                self.remote_identifiers.delete(path=fuse_path)
            FileDeleteEvent(self.events_channel).submit(
                uuid=uuid, path=fuse_path
            )
        except Exception as e:
            log.error(e)

    def getattributes(self, fuse_path):
        cache_path = self._get_path_or_dummy(fuse_path)
        if cache_path is None:
            raise FuseOSError(errno.ENOENT)
        stat = os.lstat(cache_path)
        stat_dict = dict(
            (key, getattr(stat, key))
            for key in (
                "st_atime",
                "st_ctime",
                "st_gid",
                "st_mode",
                "st_mtime",
                "st_nlink",
                "st_size",
                "st_uid",
            )
        )
        if not os.path.isdir(cache_path):
            # If not a directory
            stat_dict["st_atime"] = self.metadata_store.get_access_time(
                path=fuse_path
            )
            stat_dict["st_mtime"] = self.metadata_store.get_modification_time(
                path=fuse_path
            )
            stat_dict["st_ctime"] = self.metadata_store.get_change_time(
                path=fuse_path
            )
            print(stat_dict)
        return stat_dict

    @staticmethod
    def is_link(cache_path):
        return os.path.islink(cache_path)

    def replace_dummy(self, path):
        with PathLock(path, lock_creator="replace_dummy"):
            self._replace_dummy(path)
            FileLoadedIntoCacheEvent(self.events_channel).submit(path=path)

    def _replace_dummy(self, path):
        print(f"Replacing dummy [path]")
        if not self.states.current_state_is_remote(path):
            log.warn(
                f"Cannot replace dummy for path {path}"
                "because file is not remote."
            )
            return
        self.states.remote_to_clean(path)
        cache_path = self.converter.to_cache_path(path)
        uuid = self.remote_identifiers.get_uuid_or_none(path)
        with open(cache_path, "w+b") as file:
            try:
                file.write(self.api.download(uuid).read())
            except ConnectionError:
                raise FuseOSError(errno.ENETUNREACH)

    def create_dummy(self, path):
        with PathLock(path, lock_creator="create_dummy"):
            # This can happen if the file was written to in the meantime
            if not os.path.isfile(self.converter.to_cache_path(path)):
                raise PathDoesNotExistException(
                    f"Cannot creat dummy because {path} not found"
                )
            self.states.clean_to_remote(path)
            FileEvictedFromCacheEvent(self.events_channel).submit(path=path)

    def statfs(self, path):
        cache_path = self._get_path_or_dummy(path)
        cache_stat_info = os.statvfs(cache_path)
        stat_info = {
            key: getattr(cache_stat_info, key)
            for key in (
                "f_bavail",
                "f_bfree",
                "f_blocks",
                "f_bsize",
                "f_favail",
                "f_ffree",
                "f_files",
                "f_flag",
                "f_frsize",
                "f_namemax",
            )
        }

        # Todo: Remove some keys from above's list and set them here
        # With info from the metadata store.
        # stat_info[""]

        return stat_info


def on_cache_path_or_dummy(func):

    def using_cache_path_or_dummy(self, fuse_path, *args, **kwargs):
        print(func, fuse_path, args, kwargs)
        cache_path = self.cache._get_path_or_dummy(fuse_path)
        return func(self, cache_path, *args, **kwargs)

    return using_cache_path_or_dummy


def on_cache_path_enforce_local(func):

    def using_cache_path_enforce_local(self, fuse_path, *args, **kwargs):
        print(func, fuse_path, args, kwargs)
        cache_path = self.cache._get_path(fuse_path)
        return func(self, cache_path, *args, **kwargs)

    return using_cache_path_enforce_local


def on_cache_path(func):

    def using_cache_path(self, fuse_path, *args, **kwargs):
        print(func, fuse_path, args, kwargs)
        cache_path = self.cache.converter.to_cache_path(fuse_path)
        return func(self, cache_path, *args, **kwargs)

    return using_cache_path
