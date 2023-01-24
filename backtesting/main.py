"""Tipster backtesting app."""

import os

import emoji
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery

THIS_DIR = os.path.dirname(__file__)

creds = service_account.Credentials.from_service_account_info(
    info=st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=creds)


# @st.experimental_memo
def read_query(path):
    """Run query on Bigquery"""
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
    """Transform data"""
    return (
        dataframe.set_index("start_at")["result"]
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


st.title("Tipster Backtesting")

bookmakers_query = """SELECT key, name FROM tipster.stg_bookmaker ORDER BY name"""
all_bookmakers = run_query(bookmakers_query).set_index("key")["name"].to_dict()
leagues_query = "SELECT key, name FROM tipster.stg_league ORDER BY key"
all_leagues = run_query(leagues_query).set_index("key")["name"].to_dict()

bookmakers = st.sidebar.multiselect(
    "Bookmakers",
    options=all_bookmakers,
    format_func=lambda x: all_bookmakers[x],
    default=all_bookmakers,
)
leagues = st.sidebar.multiselect(
    "Leagues",
    options=all_leagues,
    format_func=lambda x: emoji.emojize(
        all_leagues[x]
        .replace(":England:", ":United_Kingdom:")
        .replace(":Scotland:", ":United_Kingdom:")
    ),
    default=all_leagues,
)


if not bookmakers or not leagues:
    st.stop()


query = read_query(os.path.join(THIS_DIR, "query.sql"))
query = query.format(
    bookmakers="','".join([str(b) for b in bookmakers]),
    leagues=",".join([str(l) for l in leagues]),
    kelly="{kelly}",
)

data = run_query(query)
data = transform_data(data)

fig = go.Figure(
    data=[
        go.Candlestick(
            x=data["date"],
            open=data["open"],
            high=data["high"],
            low=data["low"],
            close=data["close"],
        )
    ]
)

fig.update_layout(xaxis_rangeslider_visible=False)
st.plotly_chart(fig)
