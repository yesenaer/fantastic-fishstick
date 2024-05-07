import duckdb

def create_table(cursor: duckdb.DuckDBPyConnection, tablename: str, filepath: str):
    create_stmt = f"""CREATE TABLE {tablename} (
        Date DATE,
        Open DOUBLE,
        High DOUBLE,
        Low DOUBLE,
        Close DOUBLE,
        AdjustedClose DOUBLE,
        Volume INT64
    );
    COPY {tablename} FROM '{filepath}';"""

    cursor.execute(create_stmt)


if __name__ == '__main__':
    cursor = duckdb.connect()
    create_table(cursor, 'eurusd', './data/EURUSD-historic.csv')
    print(cursor.execute("SELECT * FROM eurusd LIMIT 10").fetchall())
