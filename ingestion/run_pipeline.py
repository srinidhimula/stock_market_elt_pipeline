from google.cloud import bigquery
import pandas as pd

from extract_api import fetch_daily_prices
from load_bigquery import create_table_if_not_exists, load_dataframe
import config


def main():
    client = bigquery.Client(project=config.PROJECT_ID)
    table_id = f"{config.PROJECT_ID}.{config.DATASET}.{config.TABLE}"

    create_table_if_not_exists(client, table_id)

    all_data = []

    for symbol in config.SYMBOLS:
        print(f"Fetching data for {symbol}")
        df = fetch_daily_prices(symbol, config.API_KEY)
        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)

    print("Preview:")
    print(final_df.head())

    load_dataframe(client, final_df, table_id)


if __name__ == "__main__":
    main()
