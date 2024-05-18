import yfinance as yf
import pandas as pd 


def retriever(tickers: str, filename: str, period: str = '1d', interval: str = '1d') -> None:
    """retriever will try to download the tickers data and store it as csv file.

    Args:
        tickers (str): the tickers to download.
        filename (str): the filename to store the historic data as. currently forces storage in data dir.
        period (str, optional): the period to download the ticker data for. Defaults to '1d'.
        interval (str, optional): the interval to download the ticker data for. Defaults to '1d'.
    """    
    if not filename.endswith('.csv'):
        filename = f'{filename}.csv'
    data: pd.DataFrame = yf.download(tickers=tickers, period=period, interval=interval)
    data.to_csv(f'./data/{filename}')
