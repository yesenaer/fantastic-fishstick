import yfinance as yf

data = yf.download("EURUSD=X", period='1d')

print(data)
