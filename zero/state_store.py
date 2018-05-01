import sqlite3

class STATES:
    DIRTY = 'DIRTY'
    CLEANING = 'CLEANING'
    TODELETE = 'TODELETE'
    DELETING = 'DELETING'

class StateStore:
    def __init__(self):
        self.connection = sqlite3.connect('state.db')
        with self.connection:
            self.connection.execute(
                '''CREATE TABLE IF NOT EXISTS states (nodepath text primary key, state text)'''
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


    def _path_has_state(self, path, state):
        cursor = self.connection.execute(
            '''SELECT state FROM states WHERE nodepath = ? AND state = ?''',
            (path, state)
        )
        return cursor.fetchone() is None

    def _set_state_on_path(self, path, state):
        with self.connection:
            self.connection.execute(
                '''INSERT OR REPLACE INTO states (nodepath, state) VALUES (?, ?)''', (path, state)
            )
