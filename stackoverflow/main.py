"""Google Cloud Function to extract soccer power index from fivethirtyeight."""

import datetime
import io

import google.cloud.logging
import pandas as pd
import requests

client = google.cloud.logging.Client()
client.setup_logging()


URL = "https://projects.fivethirtyeight.com/soccer-api/club/spi_matches.csv"


def handler(*args, **kwargs):  # pylint: disable=unused-argument
    """Function handler."""
    content = requests.get(URL, timeout=None).content

    data = pd.read_csv(io.StringIO(content.decode())).convert_dtypes()
    data["loaded_at"] = datetime.datetime.now()

    data.to_gbq(destination_table="fivethirtyeight.spi", if_exists="replace")
    return {"statusCode": 200, "message": f"loaded {data.shape[0]} rows"}


if __name__ == "__main__":
    print(handler(None))
