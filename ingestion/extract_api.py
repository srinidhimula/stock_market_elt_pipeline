import requests
import pandas as pd
from datetime import datetime


def fetch_daily_prices(symbol: str, api_key: str) -> pd.DataFrame:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": "compact"
    }

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    raw = response.json()

    if "Time Series (Daily)" not in raw:
        raise ValueError(f"Unexpected API response for {symbol}: {raw}")

    data = raw["Time Series (Daily)"]

    df = pd.DataFrame(data).T.reset_index()
    df.rename(columns={
        "index": "date",
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. adjusted close": "adjusted_close",
        "6. volume": "volume"
    }, inplace=True)

    df["symbol"] = symbol
    df["date"] = pd.to_datetime(df["date"]).dt.date

    numeric_cols = ["open", "high", "low", "close", "adjusted_close", "volume"]
    df[numeric_cols] = df[numeric_cols].astype(float)

    df["ingestion_timestamp"] = datetime.utcnow()

    return df[
        [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "adjusted_close",
            "volume",
            "ingestion_timestamp",
        ]
    ]