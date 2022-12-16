"""Telegram bot for getting Cartola FC tips with Azure function."""

import os

import telegram


def handler(*args, **kwargs):
    """Send telegrams messages."""
    bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))

    text = "HellO!"
    bot.sendMessage(chat_id=os.getenv("TELEGRAM_CHAT_ID"), text=text)

    return {"statusCode": 200}
