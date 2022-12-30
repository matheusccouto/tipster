"""Telegram bot for sending betting tips."""

import os

import google.cloud.bigquery
import google.cloud.logging
import telegram

# Bot config

CMD = {
    "setbookmakers": "Set a new bookmaker",
    "deletebookmakers": "Delete a bookmaker",
    "listbookmakers": "List your bookmakers",
    "setleague": "Set a new league",
    "deleteleague": "Delete a league",
    "listleagues": "List your leagues",
    "setev": "Set your expected value threshold",
    "setkellyfraction": "Set your fraction for Kelly criterion",
    "cancel": "Cancel current command",
}


# Queries

QUERY_AVAILABLE_BOOKIE = """
    SELECT b.key, b.name
    FROM tipster.bookmaker AS b
    LEFT JOIN tipster.user_bookmaker AS u ON b.key = u.bookmaker AND user = {chat_id}
    WHERE u.user IS NULL
    ORDER BY b.name
"""
QUERY_SET_BOOKIE = """
    INSERT INTO tipster.user_bookmaker (user, bookmaker)
    VALUES ({chat_id}, '{key}')
"""
QUERY_LIST_BOOKIE = """
    SELECT bookmaker AS key, bookmaker AS name
    FROM tipster.user_bookmaker
    WHERE user = {chat_id}
    ORDER BY bookmaker
    """
QUERY_DELETE_BOOKIE = """
    DELETE FROM tipster.user_bookmaker
    WHERE user = {chat_id} AND bookmaker = '{key}'
"""

# General config.

logging_client = google.cloud.logging.Client()
logging_client.setup_logging()

bigquery_client = google.cloud.bigquery.Client()

bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))
bot.set_my_commands([(key, val) for key, val in CMD.items()])

context = {}


def run_query(query):
    """Run a query in BigQuery."""
    job = bigquery_client.query(query)
    return job.result()


def choices(chat_id, query):
    data = run_query(query.format(chat_id=chat_id))
    text = "\n".join([f"{i}. {row.name}" for i, row in enumerate(data)])
    text = f"Select a number from the list\n\n{text}"
    bot.sendMessage(chat_id=chat_id, text=text)


def _read_choice(chat_id, text, query_list, query_update):
    data = run_query(query_list.format(chat_id=chat_id))
    try:
        selected = list(data)[int(text)]
    except ValueError:
        bot.sendMessage(chat_id=chat_id, text="Type only the number")
    except IndexError:
        bot.sendMessage(chat_id=chat_id, text="Type a number from the list")
    finally:
        run_query(query_update.format(chat_id=chat_id, key=selected.key))
        return selected


def add(chat_id, text, query_available, query_set):
    selected = _read_choice(chat_id, text, query_available, query_set)
    bot.sendMessage(chat_id=chat_id, text=f"Added {selected.name}")


def delete(chat_id, text, query_current, query_delete):
    selected = _read_choice(chat_id, text, query_current, query_delete)
    bot.sendMessage(chat_id=chat_id, text=f"Deleted {selected.name}")


def list_(chat_id, query):
    data = run_query(query.format(chat_id=chat_id))
    text = "\n".join([row.name for row in data])
    text = text if text else "You do not have bookmakers"
    bot.sendMessage(chat_id=chat_id, text=text)


def handler(request):
    """Interact with the user to configurate her/his account."""

    # Only deal with POST requests.
    if request.method != "POST":
        return {"statusCode": 200}

    # Get content from message.
    update = telegram.Update.de_json(request.get_json(), bot)
    chat_id = update.message.chat.id
    text = update.message.text

    # If the user cancels, clear the context.
    if "/cancel" in text:
        context[chat_id] = None
        return {"statusCode": 200}

    # Show available bookies if the user wants to set a new one.
    if "/setbookmaker" in text:
        context[chat_id] = "/setbookmaker"
        choices(chat_id, QUERY_AVAILABLE_BOOKIE)
        return {"statusCode": 200}

    # Get user answer when setting a new bookie.
    if context.get(chat_id) == "/setbookmaker":
        add(chat_id, text, QUERY_AVAILABLE_BOOKIE, QUERY_SET_BOOKIE)
        context[chat_id] = None
        return {"statusCode": 200}

    # List bookmakers that could be deleted
    if "/deletebookmaker" in text:
        context[chat_id] = "/deletebookmaker"
        choices(chat_id, QUERY_LIST_BOOKIE)
        return {"statusCode": 200}

    # Get user answer when setting a new bookie.
    if context.get(chat_id) == "/deletebookmaker":
        delete(chat_id, text, QUERY_LIST_BOOKIE, QUERY_DELETE_BOOKIE)
        context[chat_id] = None
        return {"statusCode": 200}

    # List user's bookmakers
    if "/listbookmakers" in text:
        list_(chat_id, QUERY_LIST_BOOKIE)
        return {"statusCode": 200}

    welcome_msg = "You can control me by sending these commands:"
    cmd_msg = "\n".join(f"/{cmd} - {descr}" for cmd, descr in CMD.items())
    bot.sendMessage(chat_id=chat_id, text=welcome_msg + "\n\n" + cmd_msg)
    return {"statusCode": 200}
