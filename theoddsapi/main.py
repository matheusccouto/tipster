"""Extract odds from The Odds API."""

import datetime
import logging
import os

import google.cloud.logging
import pandas as pd
import requests

client = google.cloud.logging.Client()
client.setup_logging()


URL = "https://api.the-odds-api.com/v4/sports/{sport}/odds"
SPORTS = ["soccer_efl_champ"]


def handler(*args, **kwargs):  # pylint: disable=unused-argument
    """Google cloud function handler."""
    records = []

    for sport in SPORTS:
        response = requests.get(
            URL.format(sport=sport),
            timeout=None,
            params={
                "apiKey": os.getenv("THE_ODDS_API_KEY"),
                "regions": "eu",
                "market": "h2h",
            },
        )
        for row in response.json():
            for bookmaker in row["bookmakers"]:
                for market in bookmaker["markets"]:

                    price_home_team = [
                        outcome["price"]
                        for outcome in market["outcomes"]
                        if outcome["name"] == row["home_team"]
                    ][0]

                    price_away_team = [
                        outcome["price"]
                        for outcome in market["outcomes"]
                        if outcome["name"] == row["away_team"]
                    ][0]

                    price_draw = [
                        outcome["price"]
                        for outcome in market["outcomes"]
                        if outcome["name"] == "Draw"
                    ][0]

                    records.append(
                        {
                            "id": row["id"],
                            "sport_key": row["sport_key"],
                            "sport_title": row["sport_title"],
                            "commence_time": row["commence_time"],
                            "home_team": row["home_team"],
                            "away_team": row["away_team"],
                            "bookmaker_key": bookmaker["key"],
                            "bookmaker_title": bookmaker["title"],
                            "bookmaker_last_update": bookmaker["last_update"],
                            "market_key": market["key"],
                            "price_home_team": price_home_team,
                            "price_draw": price_draw,
                            "price_away_team": price_away_team,
                        }
                    )

    data = pd.DataFrame.from_records(records).convert_dtypes()
    data["loaded_at"] = datetime.datetime.now()
    data["bookmaker_last_update"] = pd.to_datetime(data["bookmaker_last_update"])
    data["commence_time"] = pd.to_datetime(data["commence_time"])

    data.to_gbq(destination_table="theoddsapi.odds", if_exists="append")

    logging.info("loaded %s rows", data.shape[0])
    logging.info("requests used: %s", response.headers["x-requests-used"])
    logging.info("requests remaining: %s", response.headers["x-requests-remaining"])

    return {
        "statusCode": 200,
        "message": f"loaded {data.shape[0]} rows",
        "the-odds-api-requests-used": response.headers["x-requests-used"],
        "the-odds-api-requests-remaining": response.headers["x-requests-remaining"],
    }


if __name__ == "__main__":
    print(handler(None))
