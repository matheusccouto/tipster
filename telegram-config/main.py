"""Telegram bot for sending betting tips."""

import os

import google.cloud.logging
import pandas as pd
import telegram

client = google.cloud.logging.Client()
client.setup_logging()
bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))


def handler(request):
    """Send telegrams messages."""

    if request.method != "POST":
        return {"statusCode": 200}

    update = telegram.Update.de_json(request.get_json(), bot)
    chat_id = update.message.chat.id

    if "/setbookmaker" in update.message.text:
        query = f"""
            SELECT key, name
            FROM tipster.bookmaker
            ORDER BY name
            """
        data = pd.read_gbq(query=query)
        text = "\n".join(data["name"])
        bot.sendMessage(chat_id=chat_id, text=f"Select a bookmaker from the list\n\n{text}")
        return {"statusCode": 200}

    if "/listbookmakers" in update.message.text:
        query = f"""
            SELECT bookmaker
            FROM tipster.user_bookmaker
            WHERE user = {chat_id}
            ORDER BY bookmaker
            """
        text = "\n".join(pd.read_gbq(query=query)["bookmaker"])
        text = text if text else "Please set a list one bookmaker"
        bot.sendMessage(chat_id=chat_id, text=text)
        return {"statusCode": 200}

    if "/listleague" in update.message.text:
        query = f"""
            SELECT league
            FROM tipster.user_league
            WHERE user = {chat_id}
            ORDER BY league
            """
        text = "\n".join(pd.read_gbq(query=query)["league"])
        text = text if text else "Please set a list one league"
        bot.sendMessage(chat_id=chat_id, text=text)
        return {"statusCode": 200}

    # sent.to_gbq("tipster.sent", if_exists="append")

    return {"statusCode": 200}


if __name__ == "__main__":
    print(handler())
