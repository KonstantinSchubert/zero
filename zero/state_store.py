import sqlite3


class STATES:
    DIRTY = "DIRTY"
    CLEANING = "CLEANING"
    TODELETE = "TODELETE"
    DELETING = "DELETING"


class StateStore:
    """Manages state and identifier for each path.
    The state describes whether the file is dirty/cleaning/todelte/deleting.
    The identifier is usually the same as the path. However, if multiple hard links point to the same
    inode, the identifier will be the same for all of them and be the path of one of the links.
    """

    def __init__(self):
        self.connection = sqlite3.connect("state.db")
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS states (nodepath text primary key, state text, identifier text)"""
            )

    def is_dirty(self, path):
        return self._path_has_state(path, STATES.DIRTY)

    def is_cleaning(self, path):
        return self._path_has_state(path, STATES.CLEANING)

    def is_todelete(self, path):
        return self._path_has_state(path, STATES.TODELETE)

    def is_deleting(self, path):
        return self._path_has_state(path, STATES.DELETING)

    def set_dirty(self, path):
        self._set_state_on_path(path, STATES.DIRTY)

    def set_cleaning(self, path):
        self._set_state_on_path(path, STATES.CLEANING)

    def set_todelete(self, path):
        self._set_state_on_path(path, STATES.TODELETE)

    def set_deleting(self, path):
        self._set_state_on_path(path, STATES.DELETING)

    def get_identifier(self, path):
        cursor = self.connection.execute(
            """SELECT identifier FROM states WHERE nodepath = ? """, (path,)
        )
        return cursor.fetchone()

    def _path_has_state(self, path, state):
        cursor = self.connection.execute(
            """SELECT state FROM states WHERE nodepath = ? AND state = ?""",
            (path, state),
        )
        return cursor.fetchone() is None

    def _set_state_on_path(self, path, state, identifier):
        # The distinction between identifier and path allows us
        # to handle hard links where multiple paths refer to the same inode.
        # Until we support hard links, we assume identifier = path
        if identifier is None:
            identifier = path
        with self.connection:
            self.connection.execute(
                """INSERT OR REPLACE INTO states (nodepath, state, identifier) VALUES (?, ?, ?)""",
                (path, state, identifier),
            )
