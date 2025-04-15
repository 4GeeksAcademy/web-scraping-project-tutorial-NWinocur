import os
from bs4 import BeautifulSoup
import requests
import time
import sqlite3
from sqlite3 import Connection
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pandas import DataFrame

DATABASE_FILE = "temp.db"

def download_html(site_to_get: str) -> str:
    resource_url = site_to_get
    # Request to download the file from the Internet
    response = requests.get(resource_url, time.sleep(10))

    # If the request has been executed correctly (code 200), then the HTML content of the page has been downloaded
    # if response:
    #    # We transform the flat HTML into real HTML (structured and nested, tree-like)
    #    soup = BeautifulSoup(response.text, features="lxml")
    #    soup
    return response.text  # seems overkill to use beautifulsoup for this


def transform_html(untransformed_html) -> pd.DataFrame:
    table = pd.read_html(untransformed_html)[0]  # only care about first table for now
    print(f"{table}")
    return table


def process_dataframe(unprocessed_df):
    # last row in table is a disclaimer about how recent the data is,
    # not relevant to what we're doing, so just drop that last row
    unprocessed_df = unprocessed_df.drop(unprocessed_df.index[-1])
    # Map DataFrame columns to SQLite table's columns
    unprocessed_df.columns = [col.strip() for col in unprocessed_df.columns]
    df_clean = unprocessed_df.rename(
        columns={
            "Rank": "ID",
            "Song": "SONG",
            "Artist(s)": "ARTIST",
            "Streams (billions)": "STREAMS",
            "Release date": "RELEASE_DATE",
        }
    )

    df_clean["STREAMS"] = pd.to_numeric(
        df_clean["STREAMS"], errors="coerce"
    )  # Handle conversion
    df_clean = df_clean[
        ["ID", "SONG", "ARTIST", "STREAMS", "RELEASE_DATE"]
    ]  # Enforce order
    return df_clean


def connect_to_sqlite() -> Connection:
    # Connect to the database or create it if it doesn't exist
    con = sqlite3.connect(DATABASE_FILE)
    con.execute("""DROP TABLE IF EXISTS MOSTSTREAMEDSONGS""")
    con.execute("""CREATE TABLE MOSTSTREAMEDSONGS (
    ID INT PRIMARY KEY     NOT NULL,
    SONG           TEXT    NOT NULL,
    ARTIST         TEXT    NOT NULL,
    STREAMS        REAL    NOT NULL,
    RELEASE_DATE   TEXT
    )""")
    return con


def store_in_sqlite(con: Connection, df_to_store: DataFrame):
    con.executemany(
        "INSERT INTO MOSTSTREAMEDSONGS (ID, SONG, ARTIST, STREAMS, RELEASE_DATE) VALUES (?, ?, ?, ?, ?)",
        df_to_store.itertuples(index=False, name=None),
    )
    con.commit()


def visualize_data(db_path: str = DATABASE_FILE):
    con = sqlite3.connect(db_path)

    # Load data from SQLite
    df = pd.read_sql_query("SELECT * FROM MOSTSTREAMEDSONGS", con)
    con.close()

    # Convert release date to datetime where possible
    df['RELEASE_DATE'] = pd.to_datetime(df['RELEASE_DATE'], errors='coerce')

    # Sort and take top 10 songs by streams
    top10 = df.sort_values('STREAMS', ascending=False).head(10)

    # Plot 1: Top 10 streamed songs
    plt.figure(figsize=(12, 6))
    plt.barh(top10['SONG'][::-1], top10['STREAMS'][::-1])
    plt.xlabel("Streams (Billions)")
    plt.title("Top 10 Most Streamed Songs on Spotify")
    plt.tight_layout()
    plt.show()

    # Plot 2: Streams over release date (line plot)
    df_sorted = df.dropna(subset=['RELEASE_DATE']).sort_values('RELEASE_DATE')
    plt.figure(figsize=(12, 6))
    plt.plot(df_sorted['RELEASE_DATE'], df_sorted['STREAMS'], marker='o', linestyle='-')
    plt.xlabel("Release Date")
    plt.ylabel("Streams (Billions)")
    plt.title("Streams vs. Release Date of Songs")
    plt.tight_layout()
    plt.show()

    # Plot 3: Histogram of streaming numbers
    plt.figure(figsize=(10, 5))
    plt.hist(df['STREAMS'], bins=20, edgecolor='black')
    plt.xlabel("Streams (Billions)")
    plt.ylabel("Number of Songs")
    plt.title("Distribution of Streams Across Songs")
    plt.tight_layout()
    plt.show()


site_downloaded = download_html(
    "https://en.wikipedia.org/wiki/List_of_Spotify_streaming_records"
)
table_to_process = transform_html(site_downloaded)
processed_table = process_dataframe(table_to_process)
sql_connection = connect_to_sqlite()
store_in_sqlite(sql_connection, processed_table)
visualize_data()
