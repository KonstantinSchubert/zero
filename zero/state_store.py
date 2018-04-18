import sqlite3


STATES = (
    "dirty",
    "cleaning",
    "todelete",
    "deleting"
    )

class StateStore:
    def __init__(self):
        self.connection = sqlite3.connect('state.db')
        if there is no table
            # Create table
            with self.connection:
                conn.execute('''CREATE TABLE stocks
                             (path text, state text)''')

    def has_flag(path):
        """Checks if a certain path has a flag set"""
        todo

    def is_dirty(path):
        todo

    def is_cleaning(path):
        todo

    def is_todelete(path):
        todo

    def set_dirty(path):
        todo

    def set_cleaning(path):
        todo

    def set_todelete(path):
        todo

    def set_deleting(path):
        todo
