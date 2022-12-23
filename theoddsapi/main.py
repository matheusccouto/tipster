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
SPORTS = [
    'soccer_uefa_champs_league', # Champions League
    'soccer_uefa_europa_conference_league', # Europa Conference League
    'soccer_uefa_europa_league', # Europa League
    'soccer_epl', # Premier League
    'soccer_germany_bundesliga', # Bundesliga
    'soccer_spain_la_liga', # LaLiga
    'soccer_italy_serie_a', # Serie A
    'soccer_france_ligue_one', # Ligue 1
    'soccer_netherlands_eredivisie', # Eredivisie
    'soccer_portugal_primeira_liga', # Primeira Liga
    'soccer_brazil_campeonato', # Série A
    'soccer_mexico_ligamx', # Liga MX Apertura
    'soccer_russia_premier_league', # Premier League
    'soccer_efl_champ', # Championship
    'soccer_belgium_first_div', # Pro League
    'soccer_turkey_super_league', # Süper Lig
    'soccer_usa_mls', # MLS
    'soccer_denmark_superliga', # Superliga
    'soccer_spl', # Premiership
    'soccer_switzerland_superleague', # Super League
    'soccer_argentina_primera_division', # Primera División
    'soccer_japan_j_league', # J League
    'soccer_norway_eliteserien', # Eliteserien
    'soccer_france_ligue_two', # Ligue 2
    'soccer_spain_segunda_division', # LaLiga 2
    'soccer_italy_serie_b', # Serie B
    'soccer_germany_bundesliga2', # 2. Bundesliga
    'soccer_sweden_allsvenskan', # Allsvenskan
    'soccer_england_league1', # League One
    'soccer_australia_aleague', # A-League
    'soccer_china_superleague', # Super League
    'soccer_england_league2', # League Two
]


def handler(*args, **kwargs):  # pylint: disable=unused-argument
    """Google cloud function handler."""
    records = []

    for sport in SPORTS:
        logging.info("request %s", sport)
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
