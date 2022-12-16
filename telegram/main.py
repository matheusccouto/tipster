"""Telegram bot for getting Cartola FC tips with Azure function."""

import os

import google.cloud.logging
import telegram

client = google.cloud.logging.Client()
client.setup_logging()


def handler(*args, **kwargs):  # pylint: disable=unused-argument
    """Send telegrams messages."""
    bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))

    text = "Hello! This is a test..."
    bot.sendMessage(chat_id=os.getenv("TELEGRAM_CHAT_ID"), text=text)

    return {"statusCode": 200}
