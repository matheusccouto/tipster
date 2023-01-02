"""Telegram bot for sending betting tips."""

import os

import emoji
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
    "setbankroll": "Set your bankroll",
    "setkellyfraction": "Set your fraction for Kelly criterion",
    "cancel": "Cancel current command",
}


# Queries

QUERY_AVAILABLE_BOOKIE = """
    SELECT b.key, b.name
    FROM tipster.bookmaker AS b
    LEFT JOIN tipster.stg_user_bookmaker AS u ON b.key = u.key AND user = {chat_id}
    WHERE u.user IS NULL
    ORDER BY b.name
"""
QUERY_SET_BOOKIE = """
    INSERT INTO tipster.user_bookmaker (user, bookmaker)
    VALUES ({chat_id}, '{key}')
"""
QUERY_LIST_BOOKIE = """
    SELECT key, name
    FROM tipster.stg_user_bookmaker
    WHERE user = {chat_id}
    ORDER BY name
    """
QUERY_DELETE_BOOKIE = """
    DELETE FROM tipster.user_bookmaker
    WHERE user = {chat_id} AND bookmaker = '{key}'
"""
QUERY_AVAILABLE_LEAGUE = """
    SELECT l.id AS key, concat(f.emoji, ' ', l.tipster) AS name
    FROM `tipster.league` AS l
    LEFT JOIN `tipster.stg_user_league` AS u ON l.id = u.key AND user = {chat_id}
    LEFT JOIN `tipster.flag` AS f on l.country = f.country
    WHERE u.user IS NULL AND l.theoddsapi IS NOT NULL
    ORDER BY l.id
"""
QUERY_SET_LEAGUE = """
    INSERT INTO tipster.user_league (user, league)
    VALUES ({chat_id}, {key})
"""
QUERY_LIST_LEAGUE = """
    SELECT key, name
    FROM tipster.stg_user_league
    WHERE user = {chat_id}
    ORDER BY key
    """
QUERY_DELETE_LEAGUE = """
    DELETE FROM tipster.user_league
    WHERE user = {chat_id} AND league = {key}
"""
QUERY_SET_EV = """
    INSERT INTO tipster.user_ev (user, ev, updated_at)
    VALUES ({chat_id}, {value}, current_timestamp())
"""
QUERY_SET_BANKROLL = """
    INSERT INTO tipster.user_bankroll (user, bankroll, updated_at)
    VALUES ({chat_id}, {value}, current_timestamp())
"""
QUERY_SET_KELLY= """
    INSERT INTO tipster.user_kelly (user, fraction, updated_at)
    VALUES ({chat_id}, {value}, current_timestamp())
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


def send_message(bot, chat_id, text):
    text = emoji.emojize(text.encode("raw-unicode-escape").decode("unicode-escape"))
    bot.sendMessage(chat_id=chat_id, text=text)


def choices(chat_id, query):
    data = list(run_query(query.format(chat_id=chat_id)))
    if len(data) == 0:
        send_message(bot, chat_id, "There is nothing to be selected")
        context[chat_id] = None
    else:
        text = "\n".join([f"{i}. {row.name}" for i, row in enumerate(data)])
        text = f"Select a number from the list or 'all'\n\n{text}"
        send_message(bot, chat_id, text)


def _read_choice(chat_id, text, query_list, query_update):
    data = list(run_query(query_list.format(chat_id=chat_id)))
    try:
        if text.strip().lower() == "all":
            rows = range(len(data))
        else:
            rows = int(text)
        for i in rows:
            selected = data[i]
            run_query(query_update.format(chat_id=chat_id, key=selected.key))
            send_message(bot, chat_id=chat_id, text=f"Selected {selected.name}")
            context[chat_id] = None
    except ValueError:
        bot.sendMessage(chat_id=chat_id, text="Type only the number")
    except IndexError:
        bot.sendMessage(chat_id=chat_id, text="Type a number from the list")


def add(chat_id, text, query_available, query_set):
    _read_choice(chat_id, text, query_available, query_set)


def delete(chat_id, text, query_current, query_delete):
    _read_choice(chat_id, text, query_current, query_delete)


def list_(chat_id, query):
    data = run_query(query.format(chat_id=chat_id))
    text = "\n".join([row.name for row in data])
    text = text if text else "You do not have any"
    send_message(bot, chat_id, text)


def set_value(chat_id, query, value):
    run_query(query.format(chat_id=chat_id, value=value))
    context[chat_id] = None
    send_message(bot, chat_id=chat_id, text=f"Set {value}")


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
        if context.get(chat_id) is None:
            send_message(bot, chat_id, "There is nothing to be canceled")
        else:
            send_message(bot, chat_id, f"Canceled {context.get(chat_id)}")
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
        return {"statusCode": 200}

    # List bookmakers that could be deleted
    if "/deletebookmaker" in text:
        context[chat_id] = "/deletebookmaker"
        choices(chat_id, QUERY_LIST_BOOKIE)
        return {"statusCode": 200}

    # Get user answer when setting a new bookie.
    if context.get(chat_id) == "/deletebookmaker":
        delete(chat_id, text, QUERY_LIST_BOOKIE, QUERY_DELETE_BOOKIE)
        return {"statusCode": 200}

    # List user's leagues
    if "/listbookmakers" in text:
        list_(chat_id, QUERY_LIST_BOOKIE)
        return {"statusCode": 200}

    # Show available leagues if the user wants to set a new one.
    if "/setleague" in text:
        context[chat_id] = "/setleague"
        choices(chat_id, QUERY_AVAILABLE_LEAGUE)
        return {"statusCode": 200}

    # Get user answer when setting a new league.
    if context.get(chat_id) == "/setleague":
        add(chat_id, text, QUERY_AVAILABLE_LEAGUE, QUERY_SET_LEAGUE)
        return {"statusCode": 200}

    # List leagues that could be deleted
    if "/deleteleague" in text:
        context[chat_id] = "/deleteleague"
        choices(chat_id, QUERY_LIST_LEAGUE)
        return {"statusCode": 200}

    # Get user answer when setting a new league.
    if context.get(chat_id) == "/deletebookmaker":
        delete(chat_id, text, QUERY_LIST_LEAGUE, QUERY_DELETE_LEAGUE)
        return {"statusCode": 200}

    # List user's leagues
    if "/listleagues" in text:
        list_(chat_id, QUERY_LIST_LEAGUE)
        return {"statusCode": 200}
    
    # Set EV threshold
    if "/setev" in text:
        context[chat_id] = "/setev"
        send_message(bot, chat_id, "Type the value you want to set")
        return {"statusCode": 200}

    # Get user answer when setting EV.
    if context.get(chat_id) == "/setev":
        set_value(chat_id, QUERY_SET_EV, text)
        return {"statusCode": 200}

    # Set bankroll
    if "/setbankroll" in text:
        context[chat_id] = "/setbankroll"
        send_message(bot, chat_id, "Type the value you want to set")
        return {"statusCode": 200}

    # Get user answer when setting bankroll.
    if context.get(chat_id) == "/setbankroll":
        set_value(chat_id, QUERY_SET_BANKROLL, text)
        return {"statusCode": 200}

    # Set kelly fraction
    if "/setkellyfraction" in text:
        context[chat_id] = "/setkellyfraction"
        send_message(bot, chat_id, "Type the value you want to set")
        return {"statusCode": 200}

    # Get user answer when setting kelly fraction.
    if context.get(chat_id) == "/setkellyfraction":
        set_value(chat_id, QUERY_SET_KELLY, text)
        return {"statusCode": 200}

    welcome_msg = "You can control me by sending these commands:"
    cmd_msg = "\n".join(f"/{cmd} - {descr}" for cmd, descr in CMD.items())
    send_message(bot, chat_id, welcome_msg + "\n\n" + cmd_msg)
    return {"statusCode": 200}
