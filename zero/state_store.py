import sqlite3


class STATES:
    DIRTY = "DIRTY"
    CLEANING = "CLEANING"
    TODELETE = "TODELETE"
    DELETING = "DELETING"


class StateStore:

    def __init__(self):
        self.connection = sqlite3.connect("state.db")
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS states (nodepath text primary key, state text)"""
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
        self._insert_state_on_path(path, STATES.DIRTY)

    def set_cleaning(self, path):
        self._update_state_on_path(path, STATES.CLEANING, STATES.DIRTY)

    def set_todelete(self, path):
        self._insert_state_on_path(path, STATES.TODELETE)

    def set_deleting(self, path):
        self._update_state_on_path(path, STATES.DELETING, STATES.TODELETE)

    def set_clean(self, path):
        self.remove(path, prev_state=STATES.CLEANING)

    def set_deleted(self, path):
        self.remove(path, prev_state=STATES.TODELETE)

    def _remove(self, path, prev_state):
        todo: check that the current state is the "prev state"
        THis is basically a state transition from
        self.connection.execute(
            """DELETE from states WHERE nodepath = ?""", (path,)
        )

    def _path_has_state(self, path, state):
        cursor = self.connection.execute(
            """SELECT state FROM states WHERE nodepath = ? AND state = ?""",
            (path, state),
        )
        return cursor.fetchone() is None

    def _insert_state_on_path(self, path, state):
        # This only works if row does not yet exist.
        with self.connection:

            Todo: Check that inserting row does not yet exist.

            self.connection.execute(
                """INSERT INTO states (nodepath, state) VALUES (?, ?)""",
                (path, state),
            )
            # Raise error if not exactly one row affectd
            # Because we are calling this within  the context manager,
            # the count should refer to the above update.
            count = self.connection.execute("""SELECT changes()""")
            if not count == 1:
                raise

    def _update_state_on_path(self, path, state, prev_state):
        # Perform a state transition
        self.connection.execute(
            """UPDATE states SET state = ? WHERE nodepath = ? AND state = ? """,
            (state, path, prev_state),
        )
        # Raise error if not exactly one row affected
        # Because we are calling this within  the context manager,
        # the count should refer to the above update.
        count = self.connection.execute("""SELECT changes()""")
        if not count == 1:
            raise
