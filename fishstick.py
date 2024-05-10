import duckdb

def create_table(cursor: duckdb.DuckDBPyConnection, tablename: str, filepath: str) -> None:
    """create_table will generate a table from a file.

    Args:
        cursor (duckdb.DuckDBPyConnection): the DuckDB cursor.
        tablename (str): name of table that is to be created.
        filepath (str): location of the data file.
    """    
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
    
def add_moving_average(tablename: str, input_col: str, days: int, avg_col_name: str=None) -> None:
    """add_moving_average will add a column with calculated moving average.

    Args:
        tablename (str): table to add the moving average to.
        input_col (str): the column to calculate the moving average on.
        days (int): the amount of days to take into account for the moving average.
        avg_col_name (str, optional): name of the added average column. Defaults to: '{input_col}MA{days}'.
    """    
    if not avg_col_name:
        avg_col_name = f"{input_col}MA{str(days)}"
    cursor.execute(f"""ALTER TABLE {tablename} ADD {avg_col_name} DOUBLE""")
    populate_column_with_rolling_window(tablename, input_col, days, avg_col_name, 'AVG')
    

def add_standard_deviation(tablename: str, input_col: str, days: int, std_col_name: str=None) -> None:
    """add_standard_deviation will add a column with calculated standard diviation.

    Args:
        tablename (str): table to add the standard deviation to.
        input_col (str): the column to calculate the standard deviation on.
        days (int): the amount of days to take into account for the standard deviation.
        std_col_name (str, optional): name of the added standard deviation column. Defaults to: '{input_col}STD{days}'.
    """    
    if not std_col_name:
        std_col_name = f"{input_col}STD{str(days)}"
    cursor.execute(f"""ALTER TABLE {tablename} ADD {std_col_name} DOUBLE""")
    populate_column_with_rolling_window(tablename, input_col, days, std_col_name, 'STDDEV')


def populate_column_with_rolling_window(tablename: str, input_col: str, days: str, target_col: str, action: str) -> None:
    """populate_column_with_rolling_window uses a rolling window to calculate and add a column to an existing table.

    Args:
        tablename (str): the table to add the column to.
        input_col (str): the column to calculate the rolling window on.
        days (str): the amount of days to take into account for the rolling window.
        target_col (str): name of the column that is to be created.
        action (str): action to perform on top of the rolling window. Suggestions: AVG, STDDEV.
    """    
    stmt = f"""WITH cte AS (
                SELECT Date,
                {action}({input_col}) OVER (
                    ORDER BY Date 
                    ROWS BETWEEN {days} PRECEDING AND CURRENT ROW
                    )  AS target
                FROM {tablename}
                )

            UPDATE {tablename}
            SET {target_col} = cte.target
            FROM cte 
            WHERE {tablename}.Date = cte.Date;
                        
    """
    cursor.execute(stmt)


if __name__ == '__main__':
    cursor = duckdb.connect()
    tablename = 'eurusd'
    create_table(cursor, tablename, './data/EURUSD-historic.csv')

    for day in [20, 30]:
        add_moving_average(tablename, 'AdjustedClose', day)
        add_standard_deviation(tablename, 'AdjustedClose', day)

    print(cursor.sql(f"SELECT * FROM {tablename} LIMIT 35").df())
