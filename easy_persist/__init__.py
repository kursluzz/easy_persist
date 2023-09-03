import pickle
import os
import sqlite3


class DiskStorageConnection:
    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(self.path)

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


class DiskStorage:
    """saves data on disk. usage:
    ds = DiskStorage('storage_name')
    ds['some key'] = {1, 2, 3}
    print(ds['some key'])
    ds['some key'] |= {4} # add value to the set and save
    ds['some key'] |= {4} # remove value from the set and save
    supports all picklable formats
    https://docs.python.org/3.6/library/pickle.html"""

    def __init__(self, name, path="/tmp"):
        self.path = os.path.join(path, f"{name}.db")
        with self._connection() as conn:
            c = conn.cursor()
            c.execute(
                """CREATE TABLE IF NOT EXISTS ds (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT
            )"""
            )
            conn.commit()

    def __getitem__(self, key):
        with self._connection() as conn:
            c = conn.cursor()
            c = c.execute("SELECT value FROM ds WHERE key=?", (key,))
            row = c.fetchone()
            return pickle.loads(row[0]) if row else None

    def __setitem__(self, key, value):
        with self._connection() as conn:
            c = conn.cursor()
            value = pickle.dumps(value)
            c.execute("REPLACE INTO ds (key, value) VALUES (?, ?)", (key, value))
            conn.commit()

    def _connection(self):
        return DiskStorageConnection(self.path)


if __name__ == "__main__":
    ds = DiskStorage("my_db", "/home/oleg")
    ds["users"] = [{"user1": "aaa"}, {"user1": "aaa"}]
    print(ds["users"])
