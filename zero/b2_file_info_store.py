import sqlite3


class FileInfoStore:

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS b2_file_info (inode text primary key, file_id text)"""
            )

    def set_file_id(self, inode, file_id):
        with self.connection:
            self.connection.execute(
                """INSERT OR REPLACE INTO b2_file_info (inode, file_id) VALUES (?, ?)""",
                (inode, file_id),
            )

    def get_file_id(self, inode):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT file_id FROM b2_file_info WHERE inode = ?""", (inode,)
            )
        result = cursor.fetchone()
        return result and result[0]

    def remove_entry(self, inode):
        with self.connection:
            self.connection.execute(
                """DELETE from b2_file_info WHERE inode = ?""", (inode,)
            )
