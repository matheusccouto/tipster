"""Telegram bot for sending betting tips."""

import os
from datetime import datetime

import emoji
import google.cloud.logging
import pandas as pd
import telegram

client = google.cloud.logging.Client()
client.setup_logging()
pd.options.mode.chained_assignment = None

TZ = "America/Sao_Paulo"


def emojize(string):
    """Create emojis from unicode."""
    return emoji.emojize(string.encode("raw-unicode-escape").decode("unicode-escape"))


def handler(*args, **kwargs):  # pylint: disable=unused-argument
    """Send telegrams messages."""
    bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))

    query = """
        SELECT
            *
        FROM
            tipster.dim_bets_new
        WHERE
            user = 'tipster'
        ORDER BY
            league_id, date(start_at)
    """
    data = pd.read_gbq(query=query)
    data["date"] = data["start_at"].dt.tz_convert(TZ).dt.strftime("%d/%m/%y")

    data["1"] = data.apply(
        lambda x: f"<b><u>{x['home']}</u></b>" if x["bet"] == "home" else x["home"],
        axis=1,
    )
    data["x"] = data.apply(
        lambda x: "<b><u>x</u></b>" if x["bet"] == "draw" else "x",
        axis=1,
    )
    data["2"] = data.apply(
        lambda x: f"<b><u>{x['away']}</u></b>" if x["bet"] == "away" else x["away"],
        axis=1,
    )

    for _, group in data.groupby("user"):

        for _, row in group.iterrows():

            body = (
                f"{row['1']} {row['x']} {row['2']}\n"
                f"{row['bookmaker_name']} {row['price']}"
            )

            header = emojize(f"{row['league_emoji']} {row['league_name']} {row['date']}")

            bot.sendMessage(
                chat_id=os.getenv("TELEGRAM_CHAT_ID"),
                text=f"{header}\n{body}",
                parse_mode="html",
            )

        sent = group[["user", "id", "bookmaker_key", "bet", "price", "ev"]]
        sent["sent_at"] = datetime.now()
        sent.to_gbq("tipster.sent", if_exists="append")

    return {"statusCode": 200}


if __name__ == "__main__":
    print(handler())
