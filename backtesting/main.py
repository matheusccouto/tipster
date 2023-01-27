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

creds = service_account.Credentials.from_service_account_info(info=st.secrets["gcp_service_account"])
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
        dataframe
        .sort_values("start_at")
        .set_index("start_at")["result"]
        .cumsum()
        .resample("D")
        .agg(["first", "last", "max", "min"])
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
    text=text.replace("\\n", "\n")
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

    st.set_page_config(layout="wide", initial_sidebar_state="expanded")

    st.title("Tipster Backtesting")


    # Load sidebar options.

    bookmakers_query = """SELECT key, name FROM tipster.stg_bookmaker ORDER BY name"""
    all_bookmakers = run_query(bookmakers_query).set_index("key")["name"].to_dict()

    leagues_query = "SELECT key, name FROM tipster.stg_league ORDER BY key"
    all_leagues = run_query(leagues_query).set_index("key")["name"].to_dict()


    # Sidebar

    sbl, sbr = st.sidebar.columns(2)

    start_date = sbl.date_input("Start", value=TODAY - datetime.timedelta(days=180), max_value=TODAY)
    end_date = sbr.date_input("End", value=TODAY, min_value=start_date, max_value=TODAY)

    ev = sbl.number_input(
        "EV Threshold",
        value=0.0,
        min_value=0.0,
        step=0.01,
    )
    factor = sbr.number_input(
        "Kelly Criterion Factor",
        value=1.0,
        min_value=0.0,
        max_value=1.0,
        step=0.01,
    )
    bookmakers = st.sidebar.multiselect(
        "Bookmakers",
        options=all_bookmakers,
        format_func=lambda x: format_text(all_bookmakers[x]),
        default=all_bookmakers,
    )
    leagues = st.sidebar.multiselect(
        "Leagues",
        options=all_leagues,
        format_func=lambda x: format_text(all_leagues[x]),
        default=all_leagues,
    )


    if not bookmakers or not leagues:
        st.stop()


    query = read_query(os.path.join(THIS_DIR, "query.sql"))
    query = query.format(
        bookmakers="','".join([str(b) for b in bookmakers]),
        leagues=",".join([str(l) for l in leagues]),
        factor=factor,
        ev=ev,
        kelly="{kelly}",
        start=start_date,
        end=end_date,
    )
    data = run_query(query)
    data["message"] = data["message"].apply(format_text)
    candles = transform_data(data)

    kpi_cols = st.columns(4)

    kpi = calculate_kpi(data)
    kpi_cols[0].metric("Net", f"${kpi['net']:.2f}")
    kpi_cols[1].metric("ROI", f"{100 * kpi['roi']:.1f}%")
    kpi_cols[2].metric("Investment", f"${kpi['investment']:.2f}")
    kpi_cols[3].metric("Prizes", f"${kpi['prize']:.2f}")

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
    fig.update_layout(margin=dict(l=20, r=20, t=20, b=20), xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)


    data["Bookmaker"] = data["bookmaker_key"].apply(lambda x: format_text(all_bookmakers[x]))
    data["League"] = data["league_id"].apply(lambda x: format_text(all_leagues[x]))
    data["Net"] = data["result"]

    with st.expander("Cats"):

        cats_left, cats_right = st.columns(2)

        scale = [(-kpi['net'], "red"), (0.0, "white"), (kpi['net'], "green")]
        
        fig = px.bar(
            data.groupby(["Bookmaker"], as_index=False)["Net"].sum().sort_values("Net"),
            x="Net",
            y="Bookmaker",
            color="Net",
            orientation="h",
            color_continuous_scale=scale,
        )
        cats_right.plotly_chart(fig, use_container_width=True)

        fig = px.bar(
            data.groupby(["League"], as_index=False)["Net"].sum().sort_values("Net"),
            x="Net",
            y="League",
            color="Net",
            orientation="h",
            color_continuous_scale=scale,
        )
        cats_left.plotly_chart(fig, use_container_width=True)


    with st.expander("Bets"):
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
