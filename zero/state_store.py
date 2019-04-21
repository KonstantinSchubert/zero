import sqlite3


class IllegalTransitionException(Exception):
    pass


class STATES:
    CLEAN = "CLEAN"  # File exists locally and remotely and is clean
    REMOTE = "REMOTE"  # File exists only remotely
    DIRTY = "DIRTY"  # File exists locally and is dirty
    TODELETE = "TODELETE"  # File exists only remotely and should be deleted


class StateStore:
    """ This class is NOT thread safe"""

    def __init__(self, db_name):
        self.connection = sqlite3.connect(db_name, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS states (inode integer primary key, state text)"""
            )

    def set_remote(self, inode):
        with self.connection:
            return self._transition(
                inode, previous_states=[STATES.CLEAN], next_state=STATES.REMOTE
            )

    def set_downloaded(self, inode):
        with self.connection:
            return self._transition(
                inode, previous_states=[STATES.REMOTE], next_state=STATES.CLEAN
            )

    def set_dirty(self, inode):
        with self.connection:
            self._transition(
                inode,
                previous_states=[
                    STATES.CLEAN,
                    STATES.DIRTY,
                    STATES.TODELETE,
                    None,
                ],
                next_state=STATES.DIRTY,
            )

    def set_clean(self, inode):
        with self.connection:
            self._transition(
                inode, previous_states=[STATES.DIRTY], next_state=STATES.CLEAN
            )

    def set_todelete(self, inode):
        with self.connection:
            self._transition(
                inode,
                previous_states=[
                    STATES.CLEAN,
                    STATES.DIRTY,
                    STATES.TODELETE,
                    STATES.REMOTE,
                ],
                next_state=STATES.TODELETE,
            )

    def set_deleted(self, inode):
        with self.connection:
            self._transition(
                inode, previous_states=[STATES.TODELETE], next_state=None
            )

    def get_dirty_inodes(self):
        yield from self.get_inodes_in_state(state=STATES.DIRTY)

    def get_todelete_inodes(self):
        yield from self.get_inodes_in_state(state=STATES.TODELETE)

    def get_inodes_in_state(self, state):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT inode FROM states WHERE state = ?""", (state,)
            )
        entries = cursor.fetchall()
        for entry in entries:
            yield entry[0]

    def is_remote(self, inode):
        # ugly, might want to refactor this
        try:
            with self.connection:
                self._assert_inode_has_allowed_state(inode, [STATES.REMOTE])
        except IllegalTransitionException:
            return False
        return True

    def is_clean(self, inode):
        # ugly, might want to refactor this
        try:
            with self.connection:
                self._assert_inode_has_allowed_state(inode, [STATES.CLEAN])
        except IllegalTransitionException:
            return False
        return True

    def is_dirty(self, inode):
        # ugly,need to refactor this
        try:
            with self.connection:
                self._assert_inode_has_allowed_state(inode, [STATES.DIRTY])
        except IllegalTransitionException:
            return False
        return True

    def is_todelete(self, inode):
        # ugly,need to refactor this
        try:
            with self.connection:
                self._assert_inode_has_allowed_state(inode, [STATES.TODELETE])
        except IllegalTransitionException:
            return False
        return True

    def exists(self, inode):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT inode FROM states WHERE inode = ?""", (inode,)
            )
        entries = cursor.fetchone()
        return entries is not None

    def _transition(self, inode, previous_states, next_state):
        # To make this class thread safe, obtain inode-specific lock for this method.
        if next_state is None:
            self._assert_inode_has_allowed_state(inode, previous_states)
            self._remove(inode)

        else:
            self._assert_inode_has_allowed_state(inode, previous_states)
            self._upsert_state_on_inode(inode, next_state)

    def _assert_inode_has_allowed_state(self, inode, states):
        cursor = self.connection.execute(
            """SELECT state FROM states WHERE inode = ?""", (inode,)
        )
        result = cursor.fetchone()
        if result is None:
            if None in states:
                return
            else:
                raise IllegalTransitionException(
                    f"None of the states {states} match the current state None of the inode"
                )
        (current_state,) = result
        if current_state not in states:
            raise IllegalTransitionException(
                f"None of the states {states} match the current state ({current_state})of the inode"
            )

    def _remove(self, inode):
        self.connection.execute(
            """DELETE from states WHERE inode = ?""", (inode,)
        )

    def _upsert_state_on_inode(self, inode, state):
        # Inserts row if it does not exist
        self.connection.execute(
            """INSERT OR IGNORE INTO states (inode, state) VALUES (?, ?)""",
            (inode, state),
        )
        # Updates row to have right state (redundant if prev. statement was executed)
        self.connection.execute(
            """UPDATE states SET state = ? WHERE inode = ?""", (state, inode)
        )
