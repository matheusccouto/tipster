"""Telegram bot for getting Cartola FC tips with Azure function."""

import os

import google.cloud.logging
import pandas as pd
import telegram

client = google.cloud.logging.Client()
client.setup_logging()

TZ = "America/Sao_Paulo"


def handler(*args, **kwargs):  # pylint: disable=unused-argument
    """Send telegrams messages."""
    bot = telegram.Bot(os.getenv("TELEGRAM_TOKEN"))

    data = pd.read_gbq(query="SELECT * FROM tipster.dim_bets_new WHERE user = 'tipster'")
    data["date"] = data["start_at"].dt.tz_convert(TZ).dt.strftime("%d/%m/%Y")

    data["1"] = data.apply(
        lambda x: f"<b><u>{x['home']}</u></b>" if x["bet"] == "home" else x["home"],
        axis=1,
    )
    data["x"] = data.apply(
        lambda x: "<b><u>x</u></b>" if x["bet"] == "draw" else "x",
        axis=1,
    )
    data["2"] = data.apply(
        lambda x: f"<b><u>{x['away']}</u></b>" if x["bet"] == "away" else x["away"],
        axis=1,
    )

    for _, user_data in data.groupby("user"):

        paragraphs = []
        for (league, date), group in user_data.groupby(["date", "league_name"]):

            header = f"{date} {league}"
            body = "\n\n".join(
                group.sort_values("start_at").apply(
                    lambda x: f"{x['1']} {x['x']} {x['2']}\n{x['bookmaker_name']} {x['price']}",
                    axis=1,
                )
            )
            paragraphs.append(f"{header}\n\n{body}")

        bot.sendMessage(
            chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            text="\n\n\n".join(paragraphs),
            parse_mode="html",
        )
        user_data[["user", "id"]].to_gbq("tipster.sent", if_exists="append")

    return {"statusCode": 200}


if __name__ == "__main__":
    print(handler())
