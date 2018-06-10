import sqlite3


class FileInfoStore:

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS b2_file_info (identifier text primary key, file_id text)"""
            )

    def set_file_id(self, identifier, file_id):
        with self.connection:
            self.connection.execute(
                """INSERT OR REPLACE INTO b2_file_info (identifier, file_id) VALUES (?, ?)""",
                (identifier, file_id),
            )

    def get_file_id(self, identifier):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT file_id FROM b2_file_info WHERE identifier = ?""",
                (identifier,),
            )
        return cursor.fetchone()[0]
