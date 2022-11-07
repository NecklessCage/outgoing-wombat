
import sqlite3 as sl
import pandas as pd


def connect_to_sqlite3(filepath: str) -> sl.Connection:
    return sl.connect(filepath)


def insert(table: str, con: sl.Connection, df: pd.DataFrame, if_exists: str = 'append', chunksize: int = 1000) -> None:
    # This will need exception handling but not sure how yet.
    df.to_sql(table, con, if_exists=if_exists, chunksize=chunksize)


def fetch(con: sl.Connection, query: str) -> pd.DataFrame:
    return pd.read_sql(query, con)
