"""Tipster backtesting app."""

import datetime
import os

import emoji
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery

THIS_DIR = os.path.dirname(__file__)
TODAY = datetime.datetime.today()

creds = service_account.Credentials.from_service_account_info(
    info=st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=creds)


def read_query(path):
    """Read query from file."""
    with open(path, encoding="utf-8") as file:
        return file.read()


@st.experimental_memo(ttl=900)
def run_query(query_):
    """Run query on Bigquery"""
    query_job = client.query(query_)
    result = query_job.result()
    rows = [dict(row) for row in result]
    return pd.DataFrame.from_records(rows)


@st.experimental_memo
def transform_data(dataframe):
    """Transform data to candle bars plot"""
    return (
        dataframe.sort_values("start_at")
        .set_index("start_at")["result"]
        .cumsum()
        .resample("D")
        .agg(["first", "last", "max", "min"])
        .round(2)
        .reset_index()
        .rename(
            columns={
                "start_at": "date",
                "first": "open",
                "last": "close",
                "max": "high",
                "min": "low",
            }
        )
    )


def format_text(text):
    """Format text with newlines and emojis"""
    text = text.replace(":England:", ":United_Kingdom:")
    text = text.replace(":Scotland:", ":United_Kingdom:")
    text = text.replace("\\n", "\n")
    return emoji.emojize(text)


def calculate_kpi(data):
    """Calculate KPIs"""
    res = {}
    res["net"] = data["result"].sum()
    res["roi"] = data["result"].sum() / data["amount"].sum()
    res["investment"] = data["amount"].sum()
    res["prize"] = data["prize"].sum()
    return res


def main():
    """Main execution."""

    st.set_page_config(
        page_title="Tipster",
        page_icon=":slot_machine:",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("Tipster")

    bookmakers_query = """SELECT key, name FROM tipster.stg_bookmaker ORDER BY name"""
    leagues_query = "SELECT key, name FROM tipster.stg_league ORDER BY key"
    all_bookmakers = run_query(bookmakers_query).set_index("key")["name"].to_dict()
    all_leagues = run_query(leagues_query).set_index("key")["name"].to_dict()

    data = run_query("SELECT * FROM tipster.fct_bet WHERE outcome IS NOT NULL")

    with st.form("my_form"):
        user = st.selectbox("User", options=data["user"].unique())
        data = data[data["user"] == user]

        date_range = pd.date_range(
            start=data["start_at"].min(),
            end=data["start_at"].max(),
        ).date
        dates = st.select_slider(
            "Date range",
            value=[date_range.min(), date_range.max()],
            options=date_range,
        )
        st.form_submit_button("Submit")

    data = data[
        (data["start_at"].dt.date >= dates[0]) & (data["start_at"].dt.date <= dates[1])
    ]

    data["message"] = data["message"].apply(format_text)
    candles = transform_data(data)

    with st.expander("KPIs", expanded=True):

        kpi_cols = st.columns(4)

        kpi = calculate_kpi(data)
        kpi_cols[0].metric("Net", f"${kpi['net']:.2f}")
        kpi_cols[1].metric("ROI", f"{100 * kpi['roi']:.1f}%")
        kpi_cols[2].metric("Investment", f"${kpi['investment']:.2f}")
        kpi_cols[3].metric("Prizes", f"${kpi['prize']:.2f}")

    with st.expander("Candles", expanded=True):

        fig = go.Figure(
            data=[
                go.Candlestick(
                    x=candles["date"],
                    open=candles["open"],
                    high=candles["high"],
                    low=candles["low"],
                    close=candles["close"],
                )
            ]
        )
        fig.update_layout(
            margin=dict(l=20, r=20, t=20, b=20), xaxis_rangeslider_visible=False
        )
        st.plotly_chart(fig, use_container_width=True)

    data["Bookmaker"] = data["bookmaker_key"].apply(
        lambda x: format_text(all_bookmakers[x])
    )
    data["League"] = data["league_id"].apply(lambda x: format_text(all_leagues[x]))
    data["Net"] = data["result"]

    with st.expander("Cats", expanded=True):

        cats_left, cats_right = st.columns(2)

        scale_name = "rdylgn"
        scale_range = [-kpi["net"], kpi["net"]]

        fig = px.bar(
            data.groupby(["Bookmaker"], as_index=False)["Net"].sum().sort_values("Net"),
            x="Net",
            y="Bookmaker",
            color="Net",
            orientation="h",
            color_continuous_scale=scale_name,
            range_color=scale_range,
        )
        cats_right.plotly_chart(fig, use_container_width=True)

        fig = px.bar(
            data.groupby(["League"], as_index=False)["Net"].sum().sort_values("Net"),
            x="Net",
            y="League",
            color="Net",
            orientation="h",
            color_continuous_scale=scale_name,
            range_color=scale_range,
        )
        cats_left.plotly_chart(fig, use_container_width=True)

    with st.expander("Bets", expanded=True):
        for _, row in data.sort_values("start_at", ascending=False).iterrows():
            message = row["message"].replace("$", "\$")
            result = round(row["result"], 2)

            if result > 0:
                result = f":green[\${result}]"
            else:
                result = f":red[\${result}]"

            st.markdown(f"{message} {result}")


if __name__ == "__main__":
    main()
