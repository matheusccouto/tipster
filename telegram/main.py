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


def emojize(string):
    """Create emojis from unicode."""
    return emoji.emojize(string.encode("raw-unicode-escape").decode("unicode-escape"))


def handler(*args, **kwargs):  # pylint: disable=unused-argument
    """Send telegrams messages."""
    bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))

    query = """
        SELECT
            *,
            date(start_at, 'America/Sao_Paulo') AS date
        FROM
            tipster.fct_tips
    """
    data = pd.read_gbq(query=query)

    for (_, _, user), group in data.groupby(["league_id", "date", "user"]):

        bot.sendMessage(
            chat_id=str(user),
            text=emojize("\n\n".join(group["message"])),
            parse_mode="markdown",
            disable_web_page_preview=True,
            timeout=60,
        )

        sent = group[["user", "id", "bookmaker_key", "bet", "price", "ev", "amount", "message"]]
        sent["sent_at"] = datetime.now()
        sent.to_gbq("tipster.sent", if_exists="append")

    return {"statusCode": 200}


if __name__ == "__main__":
    print(handler())
