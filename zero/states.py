import os
from .dirty_flags import DirtyFlags
from .remote_identifiers import RemoteIdentifiers
from .path_converter import PathConverter


class WrongInitialStateException(Exception):
    pass


class StateMachine:
    """ This class is NOT thread safe. We assume that the path has write-locked (or is a read lock enough?)"""

    def __init__(self, cache_folder):
        self.remote_identifiers = RemoteIdentifiers(cache_folder)
        self.dirty_flags = DirtyFlags(cache_folder)
        self.path_converter = PathConverter(cache_folder)

    def dirty_or_clean_to_dirty(self, path):
        if self.current_state_is_dirty(path):
            return
        else:
            self.clean_to_dirty(path)

    def clean_to_dirty(self, path):
        if not self.current_state_is_clean(path):
            raise WrongInitialStateException("State should be clean but is not")
        self.dirty_flags.set_dirty_flag(path=path)

    def dirty_to_clean(self, path):
        if not self.current_state_is_dirty(path):
            raise WrongInitialStateException("State should be dirty but is not")
        self.dirty_flags.remove_dirty_flag(path=path)

    def clean_to_remote(self, path):
        if not self.current_state_is_clean(path):
            raise WrongInitialStateException("State should be clean but is not")
        cache_path = self.path_converter.to_cache_path(path)
        cache_dummy_path = self.path_converter.add_dummy_ending(cache_path)
        # Re-name to preserve file permissions, user id.
        os.rename(cache_path, cache_dummy_path)

    def remote_to_clean(self, path):
        if not self.current_state_is_remote(path):
            raise WrongInitialStateException(
                "State should be remote but is not"
            )
        cache_path = self.path_converter.to_cache_path(path)
        cache_dummy_path = self.path_converter.add_dummy_ending(cache_path)
        # Re-name to preserve file permissions, user id.
        os.rename(cache_dummy_path, cache_path)

    def current_state_is_clean(self, path):
        return (not self.dirty_flags.has_dirty_flag(path)) and (
            not self.path_converter.is_dummy(
                self.path_converter.to_cache_path(path)
            )
        )

    def current_state_is_dirty(self, path):
        return self.dirty_flags.has_dirty_flag(path)

    def current_state_is_remote(self, path):
        return self.path_converter.is_dummy(
            self.path_converter.to_cache_path(path)
        )
