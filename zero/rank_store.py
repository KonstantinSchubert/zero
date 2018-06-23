import sqlite3


class RankStore:
    """ This class is NOT thread safe"""

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS ranks (nodepath text primary key, state text)"""
            )

    def change_rank_on_path(self, path, rank_delta):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT rank FROM ranks WHERE nodepath = ?""", (path,)
            )
            result = cursor.fetchone()
            rank = result and result[0] or 0
            self._set_rank_on_path(path, rank + rank_delta)

    def remove_path(self, path):
        self.connection.execute(
            """DELETE from rank WHERE nodepath = ?""", (path,)
        )

    def apply_rank_factor(self, factor):
        self.connection.execute(
            """UPDATE rank SET Quantity = Quantity * ?""", (factor,)
        )

    def _set_rank_on_path(self, path, rank):
        with self.connection:

            self.connection.execute(
                """INSERT OR REPLACE INTO ranks (nodepath, rank) VALUES (?, ?)""",
                (path, rank),
            )
