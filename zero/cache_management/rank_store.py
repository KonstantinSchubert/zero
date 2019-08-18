import sqlite3


class FILE_LOCATION:
    REMOTE = "REMOTE"
    LOCAL = "LOCAL"


class RankStore:
    """ This class is NOT thread safe"""

    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path, timeout=5)
        with self.connection:
            self.connection.execute(
                """CREATE TABLE IF NOT EXISTS ranks (path text primary key, rank real default 0, file_location text )"""
            )

    def record_access(self, path, timestamp):
        with self.connection:
            # The rank is simply the timestamp of the last usage:
            # recent usage <-> high timestamp  <-> high rank
            self.connection.execute(
                """INSERT OR REPLACE INTO ranks (path, rank, file_location) VALUES (?, ?, ?)""",
                (path, timestamp, FILE_LOCATION.LOCAL),
            )

    def remove_or_ignore(self, path):
        with self.connection:
            # Do nothing if path does not exist
            self.connection.execute(
                """DELETE from ranks WHERE path = ?""", (path,)
            )

    def apply_rank_factor(self, factor):
        with self.connection:
            self.connection.execute(
                """UPDATE ranks SET rank = rank * ?""", (factor,)
            )

    def mark_as_remote(self, path):
        with self.connection:
            # This inserts a new row in case the path does not yet exist
            self.connection.execute(
                """INSERT OR REPLACE INTO ranks (path, file_location) VALUES (?, ?)""",
                (path, FILE_LOCATION.REMOTE),
            )

    def mark_as_local(self, path):
        with self.connection:
            # This inserts a new row in case the path does not yet exist
            self.connection.execute(
                """INSERT OR REPLACE INTO ranks (path, file_location) VALUES (?, ?)""",
                (path, FILE_LOCATION.LOCAL),
            )

    def get_clean_and_low_rank_paths(self, limit):
        with self.connection:
            cursor = self.connection.execute(
                f"""SELECT path FROM ranks WHERE file_location = '{FILE_LOCATION.LOCAL}' ORDER BY rank ASC LIMIT ? """,
                (limit,),
            )
            results = cursor.fetchall()
        return [result[0] for result in results]

    def get_remote_and_high_rank_paths(self, limit):
        with self.connection:
            cursor = self.connection.execute(
                f"""SELECT path FROM ranks WHERE file_location = '{FILE_LOCATION.REMOTE}' ORDER BY rank DESC LIMIT ? """,
                (limit,),
            )
            results = cursor.fetchall()
        return [result[0] for result in results]

    def rename_all_matching_paths(self, old_partial_path, new_partial_path):
        with self.connection:
            # Find all rows where the path starts with old_path.
            # Update all rows, replaceing old_path with new_path in their path
            cursor = self.connection.execute(
                f"""SELECT path FROM ranks WHERE path LIKE '{old_partial_path}%'"""
            )
            matches = cursor.fetchall()
            for (matching_path,) in matches:
                new_path = matching_path.replace(
                    old_partial_path, new_partial_path
                )
                print(f"Replacing paths: {matching_path} to {new_path}")
                self.connection.execute(
                    """UPDATE ranks SET path=? WHERE path=?""",
                    (new_path, matching_path),
                )

    def ranks_are_sorted(self):
        with self.connection:
            cursor = self.connection.execute(
                f"""SELECT rank FROM ranks WHERE file_location = '{FILE_LOCATION.REMOTE}' ORDER BY rank DESC LIMIT 1 """
            )
            results = cursor.fetchone()
            highest_remote_rank = results and results[0]

            cursor = self.connection.execute(
                f"""SELECT rank FROM ranks WHERE file_location = '{FILE_LOCATION.LOCAL}' ORDER BY rank ASC LIMIT 1 """
            )
            results = cursor.fetchone()
            lowest_cache_rank = results and results[0]
        if (
            highest_remote_rank
            and lowest_cache_rank
            and highest_remote_rank > lowest_cache_rank
        ):
            return False
        return True
