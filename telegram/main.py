"""Telegram bot for sending betting tips."""

import os
import re
from datetime import datetime
from time import sleep

import emoji
import google.cloud.logging
import pandas as pd
import telegram

client = google.cloud.logging.Client()
client.setup_logging()
pd.options.mode.chained_assignment = None


def handler(*args, **kwargs):  # pylint: disable=unused-argument
    """Send telegrams messages."""
    bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))

    query = """
        SELECT
            *,
            date(start_at, 'America/Sao_Paulo') AS date
        FROM
            tipster.fct_tips
        ORDER BY
            bookmaker_key, league_id, start_at
    """
    data = pd.read_gbq(query=query)

    for _, row in data.iterrows():
        bot.sendMessage(
            chat_id=str(row["user"]),
            text=emoji.emojize(row["message"]).replace("\\n", "\n"),
            parse_mode="markdown",
            disable_web_page_preview=True,
            timeout=60,
        )
        sleep(20 / 60)  # Rate limit is 20 messages per minute to the same group.

    data["message"] = data["message"].apply(
        lambda x: re.sub(r"\[.+\]\(.+\)", re.findall(r"(?<=\[)(.+)(?=\])", x)[0], x)
    )
    cols = ["user", "id", "bookmaker_key", "bet", "price", "ev", "amount", "message"]
    sent = data[cols]
    sent["sent_at"] = datetime.now()
    sent.to_gbq("tipster.sent", if_exists="append")

    return {"statusCode": 200}


if __name__ == "__main__":
    print(handler())
