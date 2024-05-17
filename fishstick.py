import duckdb
import os

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
    
def add_moving_average(cursor: duckdb.DuckDBPyConnection, tablename: str, input_col: str, days: int, 
                       avg_col_name: str=None) -> None:
    """add_moving_average will add a column with calculated moving average.

    Args:
        cursor (duckdb.DuckDBPyConnection): the DuckDB cursor.
        tablename (str): table to add the moving average to.
        input_col (str): the column to calculate the moving average on.
        days (int): the amount of days to take into account for the moving average.
        avg_col_name (str, optional): name of the added average column. Defaults to: '{input_col}MA{days}'.
    """    
    if not avg_col_name:
        avg_col_name = f"{input_col}MA{str(days)}"
    cursor.execute(f"""ALTER TABLE {tablename} ADD {avg_col_name} DOUBLE""")
    populate_column_with_rolling_window(cursor, tablename, input_col, days, avg_col_name, 'AVG')
    

def add_standard_deviation(cursor: duckdb.DuckDBPyConnection, tablename: str, input_col: str, days: int, 
                           std_col_name: str=None) -> None:
    """add_standard_deviation will add a column with calculated standard diviation.

    Args:
        cursor (duckdb.DuckDBPyConnection): the DuckDB cursor.
        tablename (str): table to add the standard deviation to.
        input_col (str): the column to calculate the standard deviation on.
        days (int): the amount of days to take into account for the standard deviation.
        std_col_name (str, optional): name of the added standard deviation column. Defaults to: '{input_col}STD{days}'.
    """    
    if not std_col_name:
        std_col_name = f"{input_col}STD{str(days)}"
    cursor.execute(f"""ALTER TABLE {tablename} ADD {std_col_name} DOUBLE""")
    populate_column_with_rolling_window(cursor, tablename, input_col, days, std_col_name, 'STDDEV')


def populate_column_with_rolling_window(cursor: duckdb.DuckDBPyConnection, tablename: str, input_col: str, days: str, 
                                        target_col: str, action: str) -> None:

    """populate_column_with_rolling_window uses a rolling window to calculate and add a column to an existing table.

    Args:
        cursor (duckdb.DuckDBPyConnection): the DuckDB cursor.
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


def write_table_to_file(cursor: duckdb.DuckDBPyConnection, tablename: str, filename: str, type: str) -> None:
    """write_table_to_parquet creates a parquet file out of the table

    Args:
        cursor (duckdb.DuckDBPyConnection): the DuckDB cursor.
        tablename (str): the table to write.
        filename (str): the filename to write the data to. 
        type (str): type of file to create, supporting any of ['parquet', 'csv'].
    """
    type = type.strip().lower()
    if type not in ['parquet', 'csv']:
        return ValueError(f'Value {type} of argument "type" is not supported.')    
    if not filename.endswith(f'.{type}'):
        filename = f'{filename}.{type}'
    
    if type == 'parquet':
        export_info = "(FORMAT PARQUET)"
    else:
        export_info = "(HEADER, DELIMITER ',')"

    stmt = f"""COPY {tablename} TO '{filename}' {export_info};"""
    cursor.execute(stmt)


if __name__ == '__main__':
    cursor = duckdb.connect()
    tablename = 'eurusd'
    table_as_file = f'./data/{tablename}.csv'

    if os.path.isfile(table_as_file):
        print("Reloading table from existing dataset.")
        cursor.execute(f"""CREATE TABLE {tablename} AS SELECT * FROM read_csv('{table_as_file}');""")
    else:
        print("Creating table from raw data file.")
        create_table(cursor, tablename, './data/EURUSD-historic.csv')

        for day in [20, 30]:
            add_moving_average(cursor, tablename, 'AdjustedClose', day)
            add_standard_deviation(cursor, tablename, 'AdjustedClose', day)

    print(cursor.sql(f"SELECT * FROM {tablename} LIMIT 35").df())

    write_table_to_file(cursor, tablename, table_as_file, 'csv')
