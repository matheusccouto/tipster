"""Telegram bot for sending betting tips."""

import os
from datetime import datetime

import google.cloud.logging
import pandas as pd
import telegram

client = google.cloud.logging.Client()
client.setup_logging()


def handler(request):
    """Send telegrams messages."""
    bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))

    if request.method == "POST":
        update = telegram.Update.de_json(request.get_json(), bot)
        chat_id = update.message.chat.id
        bot.sendMessage(chat_id=chat_id, text=update.message.text)

    # query = """
    #     SELECT
    #         *,
    #         date(start_at, 'America/Sao_Paulo') AS date
    #     FROM
    #         tipster.fct_tips
    #     WHERE
    #         user = 'tipster'
    # """
    # data = pd.read_gbq(query=query)

    # sent.to_gbq("tipster.sent", if_exists="append")

    return {"statusCode": 200}


if __name__ == "__main__":
    print(handler())
