import sqlite3


class STATES:
    CLEAN = "CLEAN"  # File exists locally and remotely and is clean
    REMOTE = "REMOTE"  # File exists only remotely
    DIRTY = "DIRTY"  # File exists locally and is dirty
    CLEANING = "CLEANING"  # File exists locally and is being uploaded
    TODELETE = "TODELETE"  # File exists only remotely and should be deleted
    DELETING = (
        "DELETING"
    )  # File exists only remotely and deletion is in progress


class StateStore:
    """ This class is NOT thread safe"""

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

    def set_remote(self, path):
        return self._transition(
            path, previous_states=[STATES.CLEAN], next_state=STATES.REMOTE
        )

    def set_downloaded(self, path):
        return self._transition(
            path, previous_states=[STATES.REMOTE], next_state=STATES.CLEAN
        )

    def set_dirty(self, path):
        self._transition(
            path, previous_states=[STATES.CLEAN, None], next_state=STATES.DIRTY
        )

    def set_cleaning(self, path):
        self._transition(
            path, previous_states=[STATES.DIRTY], next_state=STATES.CLEANING
        )

    def set_clean(self, path):
        self._transition(
            path, previous_states=[STATES.CLEANING], next_state=STATES.CLEAN
        )

    def set_todelete(self, path):
        self._transition(
            path,
            previous_states=[STATES.IDLE, STATES.DIRTY],
            next_state=STATES.TODELETE,
        )

    def set_deleting(self, path):
        self._transition(
            path, previous_states=[STATES.TODELETE], next_state=STATES.DELETING
        )

    def set_deleted(self, path):
        self._transition(path, prev_states=[STATES.DELETING], next_state=None)

    def _transition(self, path, previous_states, next_state):
        # To make this class thread safe, obtain path-specific lock for this method.
        if next_state is None:
            with self.connection:
                self._assert_path_has_allowed_state(path, previous_states)
                self._remove(path)

        else:
            with self.connection:
                self._assert_path_has_allowed_state(path, previous_states)
                self._upsert_state_on_path(path, next_state)

    def _assert_path_has_allowed_state(self, path, states):

        # None is an allowed value for a previous state
        if None in states and not self._path_in_table(path):
            return
        not_none_states = [state for state in states if state is not None]
        placeholders = ",".join(len(not_none_states) * ["?"])
        cursor = self.connection.execute(
            """SELECT state FROM states WHERE nodepath = ? AND state IN ({placeholders})""".format(
                placeholders=placeholders
            ),
            tuple([path] + not_none_states),
        )
        if cursor.fetchone() is not None:
            return
        raise Exception(
            f"None of the states {states} match the state of the path"
        )

    def _remove(self, path):
        self.connection.execute(
            """DELETE from states WHERE nodepath = ?""", (path,)
        )

    def _path_in_table(self, path):
        cursor = self.connection.execute(
            """SELECT * FROM states WHERE nodepath = ?""", (path,)
        )
        return cursor.fetchone() is not None

    def _upsert_state_on_path(self, path, state):
        # This only works if row does not yet exist.
        with self.connection:

            self.connection.execute(
                """INSERT OR REPLACE INTO states (nodepath, state) VALUES (?, ?)""",
                (path, state),
            )
