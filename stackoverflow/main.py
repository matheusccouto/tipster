"""Google Cloud Function to extract soccer power index from fivethirtyeight."""

import datetime
import io

import google.cloud.logging
import pandas as pd
import requests

client = google.cloud.logging.Client()
client.setup_logging()


URLS = {
    "spi": "https://projects.fivethirtyeight.com/soccer-api/club/spi_matches_latest.csv",
    "nfl": "https://projects.fivethirtyeight.com/nfl-api/nfl_elo_latest.csv",
    "nba": "https://projects.fivethirtyeight.com/nba-model/nba_elo_latest.csv",
    "nhl": "https://projects.fivethirtyeight.com/nhl-api/nhl_elo_latest.csv",
    "mlb": "https://projects.fivethirtyeight.com/mlb-api/mlb_elo_latest.csv",
    "wnba": "https://projects.fivethirtyeight.com/wnba-api/wnba_elo_latest.csv",
}


def handler(*args, **kwargs):  # pylint: disable=unused-argument
    """Function handler."""
    for table, url in URLS.items():
        content = requests.get(url, timeout=None).content

        data = pd.read_csv(io.StringIO(content.decode())).convert_dtypes()
        data["date"] = pd.to_datetime(data["date"])
        data["loaded_at"] = datetime.datetime.now()
        data.columns = [col.replace("-", "_") for col in data.columns]

        data.to_gbq(destination_table=f"fivethirtyeight.{table}", if_exists="replace")

    return {"statusCode": 200}


if __name__ == "__main__":
    print(handler(None))
