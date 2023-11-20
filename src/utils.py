import os
import sqlite3

def set_up_database(db_name):
    """ create a database connection to a SQLite database """
    if os.path.exists(db_name):
           os.remove(db_name)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    q = """
        CREATE TABLE dim_stations (
        id TEXT PRIMARY KEY,
        name TEXT
        )
    """
    cursor.execute(q)

    q = """
        CREATE TABLE fct_rides (
            id TEXT PRIMARY KEY,
            start_station_id TEXT,
            end_station_id TEXT,
            datetime REAL,
            duration_sec REAL,
            FOREIGN KEY (start_station_id) REFERENCES dim_stations (id),
            FOREIGN KEY (end_station_id) REFERENCES dim_stations (id)
        )
    """
    cursor.execute(q)
    conn.commit()
    cursor.close()
    conn.close()

def format_name(raw_name: str) -> str:
    replace_dict = {"æ": "ae", "ø": "oe", "å": "aa", "Æ": "ae", "Ø": "oe", "Å": "aa"}
    for old, new in replace_dict.items():
        raw_name = raw_name.replace(old, new)
    return raw_name