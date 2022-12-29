"""Telegram bot for sending betting tips."""

import os

import google.cloud.bigquery
import google.cloud.logging
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

QUERY_SET_BOOKIE = """
    SELECT b.key, b.name
    FROM tipster.bookmaker AS b
    LEFT JOIN tipster.user_bookmaker AS u ON b.key = u.bookmaker AND user = {chat_id}
    WHERE u.user IS NULL
    ORDER BY b.name
    """
QUERY_LIST_BOOKIE = """
    SELECT bookmaker
    FROM tipster.user_bookmaker
    WHERE user = {chat_id}
    ORDER BY bookmaker
    """
QUERY_SET_LEAGUE = """
    SELECT b.key, b.name
    FROM tipster.bookmaker AS b
    LEFT JOIN tipster.user_bookmaker AS u ON b.key = u.bookmaker AND user = {chat_id}
    WHERE u.user IS NULL
    ORDER BY b.name
    """
QUERY_LIST_LEAGUE = """
    SELECT bookmaker
    FROM tipster.user_bookmaker
    WHERE user = {chat_id}
    ORDER BY bookmaker
    """

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


def handler(request):
    """Send telegrams messages."""

    if request.method != "POST":
        return {"statusCode": 200}

    update = telegram.Update.de_json(request.get_json(), bot)
    chat_id = update.message.chat.id
    text = update.message.text

    if "/setbookmaker" in text:
        context[chat_id] = "/setbookmaker"
        data = run_query(QUERY_SET_BOOKIE.format(chat_id=chat_id))
        text = "\n".join([f"{i}. {row.name}" for i, row in enumerate(data)])
        bot.sendMessage(
            chat_id=chat_id,
            text=f"Select a number from the list\n\n{text}",
        )
        return {"statusCode": 200}

    if "/listbookmakers" in text:
        data = run_query(QUERY_LIST_BOOKIE.format(chat_id=chat_id))
        text = "\n".join([row.bookmaker for row in data])
        text = text if text else "You do not have bookmakers"
        bot.sendMessage(chat_id=chat_id, text=text)
        return {"statusCode": 200}

    if context.get(chat_id) == "/setbookmaker":
        data = list(run_query(QUERY_SET_BOOKIE.format(chat_id=chat_id)))
        try:
            selected = data[int(text)]
        except ValueError:
            bot.sendMessage(chat_id=chat_id, text=f"You should type only the number")

        bot.sendMessage(chat_id=chat_id, text=f"Added {selected.key}")
        return {"statusCode": 200}

    # sent.to_gbq("tipster.sent", if_exists="append")

    welcome_msg = "You can control me by sending these commands:"
    cmd_msg = "\n".join(f"/{cmd} - {descr}" for cmd, descr in CMD.items())
    bot.sendMessage(chat_id=chat_id, text=welcome_msg + "\n\n" + cmd_msg)

    return {"statusCode": 200}


if __name__ == "__main__":
    print(handler())
