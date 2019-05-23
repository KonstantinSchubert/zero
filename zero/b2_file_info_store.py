import sqlite3


class FileInfoStore:

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS b2_file_info (file_uuid text primary key, file_id text)"""
            )

    def set_file_id(self, file_uuid, file_id):
        print(file_uuid, file_id)
        with self.connection:
            self.connection.execute(
                """INSERT OR REPLACE INTO b2_file_info (file_uuid, file_id) VALUES (?, ?)""",
                (file_uuid, file_id),
            )

    def get_file_id(self, file_uuid):
        with self.connection:
            cursor = self.connection.execute(
                """SELECT file_id FROM b2_file_info WHERE file_uuid = ?""",
                (file_uuid,),
            )
        result = cursor.fetchone()
        return result and result[0]

    def remove_entry(self, file_uuid):
        with self.connection:
            self.connection.execute(
                """DELETE from b2_file_info WHERE file_uuid = ?""", (file_uuid,)
            )
