import os

import pandas as pd
from pandas import DataFrame


def download_masterlist(cache: bool = False) -> pd.DataFrame:
    if cache:
        if os.path.exists("data/masterlist/masterlist_raw.csv.gz"):
            return pd.read_csv("data/masterlist/masterlist_raw.csv.gz")

    masterlist: pd.DataFrame = pd.read_csv("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt", sep=" ", header=None, low_memory=False, dtype=str, names=[0, 1, "url"])
    masterlist = masterlist.drop(columns=[0, 1])

    if cache:
        os.makedirs("data/masterlist", exist_ok=True)
        masterlist.to_csv("data/masterlist/masterlist_raw.csv.gz", index=False, compression="gzip")

    return masterlist

def process_masterlist(masterlist: pd.DataFrame, cache: bool = False) -> pd.DataFrame:
    if cache:
        if os.path.exists("data/masterlist/masterlist_processed.csv.gz"):
            return pd.read_csv("data/masterlist/masterlist_processed.csv.gz")

    masterlist["date"] = masterlist["url"].str.split("/").str[4].str.split(".", n=1).str[0]

    masterlist["year"]   = masterlist["date"].str[0:4]
    masterlist["month"]  = masterlist["date"].str[4:6]
    masterlist["day"]    = masterlist["date"].str[6:8]
    masterlist["hour"]   = masterlist["date"].str[8:10]
    masterlist["minute"] = masterlist["date"].str[10:12]
    masterlist["second"] = masterlist["date"].str[12:14]

    masterlist["type"] = masterlist["url"].str.split("/").str[4].str.split(".", n=2).str[1]

    if cache:
        os.makedirs("data/masterlist", exist_ok=True)
        masterlist.to_csv("data/masterlist/masterlist_processed.csv.gz", index=False, compression="gzip")

    return masterlist

def split_masterlist(masterlist: pd.DataFrame, cache: bool = False) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    if cache:
        if os.path.exists("data/masterlist/masterlist_events.csv.gz") and os.path.exists("data/masterlist/masterlist_mentions.csv.gz") and os.path.exists("data/masterlist/masterlist_details.csv.gz"):
            events = pd.read_csv("data/masterlist/masterlist_events.csv.gz")
            mentions = pd.read_csv("data/masterlist/masterlist_mentions.csv.gz")
            details = pd.read_csv("data/masterlist/masterlist_details.csv.gz")

            return events, mentions, details

    events: pd.DataFrame = masterlist[masterlist["type"] == "export"]
    mentions: pd.DataFrame = masterlist[masterlist["type"] == "mentions"]
    details: pd.DataFrame = masterlist[masterlist["type"] == "gkg"]

    if cache:
        os.makedirs("data/masterlist", exist_ok=True)
        events.to_csv("data/masterlist/masterlist_events.csv.gz", index=False, compression="gzip")
        mentions.to_csv("data/masterlist/masterlist_mentions.csv.gz", index=False, compression="gzip")
        details.to_csv("data/masterlist/masterlist_details.csv.gz", index=False, compression="gzip")

    return events, mentions, details

def get_years(masterlist: pd.DataFrame) -> list[int]:
    return masterlist["year"].dropna().unique().astype(int).tolist()

def split_into_years(masterlist: pd.DataFrame, years: list[int]) -> dict[int, pd.DataFrame]:
    to_return: dict[int, pd.DataFrame] = {}

    for year in years:
        to_return[year] = masterlist[masterlist["year"] == year]

    return to_return