import sqlite3


class STATES:
    IDLE = "IDLE"
    # NOt sure if IDLE is needed or I can just remmove the row (None) in this case
    DIRTY = "DIRTY"
    CLEANING = "CLEANING"
    TODELETE = "TODELETE"
    DELETING = "DELETING"


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

    def set_dirty(self, path):
        self._transition(
            path, previous_states=[STATES.IDLE, None], next_state=STATES.DIRTY
        )

    def set_cleaning(self, path):
        self._transition(
            path, previous_states=[STATES.DIRTY], next_state=STATES.CLEANING
        )

    def set_clean(self, path):
        self.remove(path, prev_states=[STATES.CLEANING], next_state=STATES.IDLE)

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
        raise Exception("None of the states match the state of the path")

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
