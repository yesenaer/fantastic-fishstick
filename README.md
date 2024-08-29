![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

# fantastic-fishstick

Small experiment with DuckDB and trading data

## Getting started 

- Clone the repo
- (Suggested) Create and activate virtual environment
- Install dependencies by running `pip install -r requirements.txt`
- Download some historic trading data and place it in the `data` dir 
    - for now table columns assume data is from Yahoo Finance
    - navigate there to get data like the EUR/USD quotes history in csv
- Update tablename and filepath in `fishstick.py` and execute 

## Config

It is possible to alter the tickers in scope using the config file.

## Download using code 

Using the package `yfinance` for downloads of historic data, if not yet present.
Suggest to not download large periods, and store downloads as files to ensure its not done every time.

## Checklist 

Idea is to create a checklist of trading decisions that need to verify the current rate and decide whether it would be a good time to buy or sell. To describe the different metrics used in the checklist, a `docs` directory is created hosting a ChatGPT description of the metric(s).
