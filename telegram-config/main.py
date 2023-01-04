"""Telegram bot for sending betting tips."""

import logging
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
    "bet": "Include this on a reply to indicated that replied bet was placed",
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
QUERY_SET_KELLY = """
    INSERT INTO tipster.user_kelly (user, fraction, updated_at)
    VALUES ({chat_id}, {value}, current_timestamp())
"""
QUERY_REGISTER_BET = """
    INSERT INTO tipster.bet (user, message, updated_at, delete)
    VALUES ({chat_id}, '{value}', current_timestamp(), FALSE)
"""
QUERY_UNREGISTER_BET = """
    INSERT INTO tipster.bet (user, message, updated_at, delete)
    VALUES ({chat_id}, '{value}', current_timestamp(), TRUE)
"""

# General config.

logging_client = google.cloud.logging.Client()
logging_client.setup_logging()

bigquery_client = google.cloud.bigquery.Client()

bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))
bot.set_my_commands(list(CMD.items()))

context = {}


def run_query(query):
    """Run a query in BigQuery."""
    logging.info("run: %s", query)
    job = bigquery_client.query(query)
    return job.result()


def choices(chat_id, query, message_id=None):
    """List available choices."""
    data = list(run_query(query.format(chat_id=chat_id)))
    if len(data) == 0:
        bot.sendMessage(
            chat_id,
            "There is nothing to be selected",
            reply_to_message_id=message_id,
        )
        context[chat_id] = None
    else:
        text = "\n".join([f"{i}. {row.name}" for i, row in enumerate(data)])
        text = f"Select a number from the list or 'all'\n\n{text}"
        bot.sendMessage(chat_id, emoji.emojize(text), reply_to_message_id=message_id)


def read_choice(chat_id, text, query_list, query_update, message_id=None):
    """Read choice."""
    data = list(run_query(query_list.format(chat_id=chat_id)))
    try:
        if text.strip().lower() == "all":
            rows = range(len(data))
        else:
            rows = [int(text)]
        for i in rows:
            selected = data[i]
            run_query(query_update.format(chat_id=chat_id, key=selected.key))
            bot.sendMessage(
                chat_id,
                text=emoji.emojize(f"Selected {selected.name}"),
                reply_to_message_id=message_id,
            )
            context[chat_id] = None
    except ValueError:
        bot.sendMessage(
            chat_id=chat_id,
            text="Type only the number",
            reply_to_message_id=message_id,
        )
    except IndexError:
        bot.sendMessage(
            chat_id=chat_id,
            text="Type a number from the list",
            reply_to_message_id=message_id,
        )


def list_(chat_id, query, message_id=None):
    """List items."""
    data = run_query(query.format(chat_id=chat_id))
    text = "\n".join([row.name for row in data])
    text = text if text else "You do not have any"
    bot.sendMessage(chat_id, emoji.emojize(text), reply_to_message_id=message_id)


def set_value(chat_id, query, value, message_id=None):
    """Set a value."""
    query = query.format(
        chat_id=chat_id,
        value=emoji.demojize(value),
    )
    run_query(query)
    context[chat_id] = None
    bot.sendMessage(chat_id, text="Done", reply_to_message_id=message_id)


def handler(request):
    """Interact with the user to configurate her/his account."""

    # Only deal with POST requests.
    if request.method != "POST":
        return {"statusCode": 200}

    # Get content from message.
    update = telegram.Update.de_json(request.get_json(), bot)
    chat_id = update.message.chat.id
    text = update.message.text
    message_id = update.message.message_id

    if update.message.reply_to_message:
        original_text = update.message.reply_to_message.text

        if "/bet" in text:
            set_value(chat_id, QUERY_REGISTER_BET, original_text, message_id)
            return {"statusCode": 200}

        if "/cancel" in text:
            set_value(chat_id, QUERY_UNREGISTER_BET, original_text, message_id)
            return {"statusCode": 200}

    # If the user cancels, clear the context.
    if "/cancel" in text:
        if context.get(chat_id) is None:
            bot.sendMessage(
                chat_id,
                "There is nothing to be canceled",
                reply_to_message_id=message_id,
            )
        else:
            bot.sendMessage(
                chat_id,
                f"Canceled {context.get(chat_id)}",
                reply_to_message_id=message_id,
            )
        context[chat_id] = None
        return {"statusCode": 200}

    # Show available bookies if the user wants to set a new one.
    if "/setbookmaker" in text:
        context[chat_id] = "/setbookmaker"
        choices(chat_id, QUERY_AVAILABLE_BOOKIE, message_id)
        return {"statusCode": 200}

    # Get user answer when setting a new bookie.
    if context.get(chat_id) == "/setbookmaker":
        read_choice(chat_id, text, QUERY_AVAILABLE_BOOKIE, QUERY_SET_BOOKIE, message_id)
        return {"statusCode": 200}

    # List bookmakers that could be deleted
    if "/deletebookmaker" in text:
        context[chat_id] = "/deletebookmaker"
        choices(chat_id, QUERY_LIST_BOOKIE, message_id)
        return {"statusCode": 200}

    # Get user answer when setting a new bookie.
    if context.get(chat_id) == "/deletebookmaker":
        read_choice(chat_id, text, QUERY_LIST_BOOKIE, QUERY_DELETE_BOOKIE, message_id)
        return {"statusCode": 200}

    # List user's leagues
    if "/listbookmakers" in text:
        list_(chat_id, QUERY_LIST_BOOKIE, message_id)
        return {"statusCode": 200}

    # Show available leagues if the user wants to set a new one.
    if "/setleague" in text:
        context[chat_id] = "/setleague"
        choices(chat_id, QUERY_AVAILABLE_LEAGUE, message_id)
        return {"statusCode": 200}

    # Get user answer when setting a new league.
    if context.get(chat_id) == "/setleague":
        read_choice(chat_id, text, QUERY_AVAILABLE_LEAGUE, QUERY_SET_LEAGUE, message_id)
        return {"statusCode": 200}

    # List leagues that could be deleted
    if "/deleteleague" in text:
        context[chat_id] = "/deleteleague"
        choices(chat_id, QUERY_LIST_LEAGUE, message_id)
        return {"statusCode": 200}

    # Get user answer when setting a new league.
    if context.get(chat_id) == "/deletebookmaker":
        read_choice(chat_id, text, QUERY_LIST_LEAGUE, QUERY_DELETE_LEAGUE, message_id)
        return {"statusCode": 200}

    # List user's leagues
    if "/listleagues" in text:
        list_(chat_id, QUERY_LIST_LEAGUE, message_id)
        return {"statusCode": 200}

    # Set EV threshold
    if "/setev" in text:
        context[chat_id] = "/setev"
        bot.sendMessage(
            chat_id,
            "Type the value you want to set",
            reply_to_message_id=message_id,
        )
        return {"statusCode": 200}

    # Get user answer when setting EV.
    if context.get(chat_id) == "/setev":
        set_value(chat_id, QUERY_SET_EV, text, message_id)
        return {"statusCode": 200}

    # Set bankroll
    if "/setbankroll" in text:
        context[chat_id] = "/setbankroll"
        bot.sendMessage(
            chat_id,
            "Type the value you want to set",
            reply_to_message_id=message_id,
        )
        return {"statusCode": 200}

    # Get user answer when setting bankroll.
    if context.get(chat_id) == "/setbankroll":
        set_value(chat_id, QUERY_SET_BANKROLL, text, message_id)
        return {"statusCode": 200}

    # Set kelly fraction
    if "/setkellyfraction" in text:
        context[chat_id] = "/setkellyfraction"
        bot.sendMessage(
            chat_id,
            "Type the value you want to set",
            reply_to_message_id=message_id,
        )
        return {"statusCode": 200}

    # Get user answer when setting kelly fraction.
    if context.get(chat_id) == "/setkellyfraction":
        set_value(chat_id, QUERY_SET_KELLY, text, message_id)
        return {"statusCode": 200}

    welcome_msg = "You can control me by sending these commands:"
    cmd_msg = "\n".join(f"/{cmd} - {descr}" for cmd, descr in CMD.items())
    bot.sendMessage(
        chat_id,
        welcome_msg + "\n\n" + cmd_msg,
        reply_to_message_id=message_id,
    )
    return {"statusCode": 200}
