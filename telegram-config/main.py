"""Telegram bot for sending betting tips."""

import os

import google.cloud.logging
import pandas as pd
import telegram

CMD = {
    "setbookmakers": "Set a new bookmaker",
    "deletebookmakers": "Delete a bookmaker",
    "listbookmakers": "List your bookmakers",
    "setleague": "Set a new league",
    "deleteleague": "Delete a league",
    "listleagues": "List your leagues",
    "setev": "Set your expected value threshold",
}

client = google.cloud.logging.Client()
client.setup_logging()

bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))
bot.set_my_commands([(key, val) for key, val in CMD.items()])

context = {}


def handler(request):
    """Send telegrams messages."""

    if request.method != "POST":
        return {"statusCode": 200}

    update = telegram.Update.de_json(request.get_json(), bot)
    chat_id = update.message.chat.id
    context[chat_id] = ""

    bot.sendMessage(chat_id=chat_id, text=f"context: {context[chat_id]}")

    if "/setbookmaker" in update.message.text:
        context[chat_id] = "/setbookmaker"
        query = f"""
            SELECT key, name
            FROM tipster.bookmaker
            ORDER BY name
            """
        data = pd.read_gbq(query=query)
        text = "\n".join(data["name"])
        bot.sendMessage(
            chat_id=chat_id, text=f"Select a bookmaker from the list\n\n{text}"
        )
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

    if "/setleague" in update.message.text:
        context[chat_id] = "/setleague"
        query = f"""
            SELECT id, tipster
            FROM tipster.league
            ORDER BY id
            """
        data = pd.read_gbq(query=query)
        text = "\n".join(data["tipster"])
        bot.sendMessage(
            chat_id=chat_id, text=f"Select a league from the list\n\n{text}"
        )
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

    if context[chat_id] == "/setbookmaker":
        bot.sendMessage(chat_id=chat_id, text=f"Added bookmaker {text}")
        return {"statusCode": 200}

    if context[chat_id] == "/deletebookmaker":
        bot.sendMessage(chat_id=chat_id, text=f"Deleted bookmaker {text}")
        return {"statusCode": 200}

    if context[chat_id] == "/setleague":
        bot.sendMessage(chat_id=chat_id, text=f"Added league {text}")
        return {"statusCode": 200}

    if context[chat_id] == "/deleteleague":
        bot.sendMessage(chat_id=chat_id, text=f"Deleted league {text}")
        return {"statusCode": 200}

    # sent.to_gbq("tipster.sent", if_exists="append")

    bot.sendMessage(chat_id=chat_id, text=f"context: {context[chat_id]}")

    # welcome_msg = "You can control me by sending these commands:"
    # cmd_msg =  "\n\n\n".join(f"/{cmd} - {descr}" for cmd, descr in CMD.items())
    # bot.sendMessage(chat_id=chat_id, text=welcome_msg + cmd_msg)

    return {"statusCode": 200}


if __name__ == "__main__":
    print(handler())
