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
    
def add_moving_average(tablename, days: int):
    days_in_str = str(days)
    cursor.execute(f"""ALTER TABLE {tablename} ADD MovingAverage{days_in_str} DOUBLE""")
    
    moving_average_stmt = f"""WITH cte AS (
                            SELECT Date,
                            avg(AdjustedClose) OVER(
                                ORDER BY Date 
                                ROWS BETWEEN {days} PRECEDING AND CURRENT ROW) AS average 
                                FROM {tablename}
                            )

                        UPDATE {tablename}
                        SET MovingAverage{days_in_str} = cte.average
                        FROM cte 
                        WHERE {tablename}.Date = cte.Date;
    """
    cursor.execute(moving_average_stmt)


if __name__ == '__main__':
    cursor = duckdb.connect()
    tablename = 'eurusd'
    create_table(cursor, tablename, './data/EURUSD-historic.csv')
    add_moving_average(tablename, 20)
    add_moving_average(tablename, 30)
    print(cursor.sql(f"SELECT * FROM {tablename} LIMIT 35").df())
