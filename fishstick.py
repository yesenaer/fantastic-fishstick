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
                    COPY {tablename} FROM '{filepath}';
    """
    cursor.execute(create_stmt)
    
def add_moving_average(tablename, input_col, days: int):
    days_in_str = str(days)
    target_col_name = f"{input_col}MA{days_in_str}"
    cursor.execute(f"""ALTER TABLE {tablename} ADD {target_col_name} DOUBLE""")
    
    moving_average_stmt = f"""WITH cte AS (
                            SELECT Date,
                            avg({input_col}) OVER(
                                ORDER BY Date 
                                ROWS BETWEEN {days} PRECEDING AND CURRENT ROW) AS average 
                                FROM {tablename}
                            )

                        UPDATE {tablename}
                        SET {target_col_name} = cte.average
                        FROM cte 
                        WHERE {tablename}.Date = cte.Date;
    """
    cursor.execute(moving_average_stmt)


if __name__ == '__main__':
    cursor = duckdb.connect()
    tablename = 'eurusd'
    create_table(cursor, tablename, './data/EURUSD-historic.csv')
    add_moving_average(tablename, 'AdjustedClose', 20)
    add_moving_average(tablename, 'AdjustedClose', 30)
    print(cursor.sql(f"SELECT * FROM {tablename} LIMIT 35").df())
