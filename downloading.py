import faulthandler
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import asyncio
import aiohttp
import io
import gzip
import zipfile
import re
import gc

from aiohttp import ClientResponseError
from pandas.errors import ParserError

from progress import make_progress

event_cols = [
    "GlobalEventID",           # 01 – unique 64-bit identifier for this event row
    "SQLDATE",                 # 02 – event date (YYYYMMDD) taken from the text
    "MonthYear",               # 03 – same date in YYYYMM format
    "Year",                    # 04 – four-digit year of the event
    "FractionDate",            # 05 – YYYY.fraction_of_year (approximate)

    # -------- actor 1 ---------------------------------------------------
    "Actor1Code",              # 06 – full CAMEO actor code (geo+role+type chain)
    "Actor1Name",              # 07 – canonical actor name (“UNITED STATES”, “HAMAS”)
    "Actor1CountryCode",       # 08 – 3-letter CAMEO country for actor 1
    "Actor1KnownGroupCode",    # 09 – if actor 1 is a known org/rebel group
    "Actor1EthnicCode",        # 10 – ethnic affiliation code (rarely filled)
    "Actor1Religion1Code",     # 11 – primary religion code
    "Actor1Religion2Code",     # 12 – secondary religion code (e.g., Catholic)
    "Actor1Type1Code",         # 13 – primary role / type (GOV, BUS, MIL, REF …)
    "Actor1Type2Code",         # 14 – secondary role / qualifier
    "Actor1Type3Code",         # 15 – tertiary role (seldom used)

    # -------- actor 2 ---------------------------------------------------
    "Actor2Code",              # 16 – full CAMEO code for actor 2 (may be blank)
    "Actor2Name",              # 17 – canonical name of actor 2
    "Actor2CountryCode",       # 18 – country of actor 2
    "Actor2KnownGroupCode",    # 19 – known org code for actor 2
    "Actor2EthnicCode",        # 20 – ethnic code for actor 2
    "Actor2Religion1Code",     # 21 – primary religion code actor 2
    "Actor2Religion2Code",     # 22 – secondary religion code actor 2
    "Actor2Type1Code",         # 23 – primary role / type actor 2
    "Actor2Type2Code",         # 24 – secondary role actor 2
    "Actor2Type3Code",         # 25 – tertiary role actor 2

    # -------- event action attributes ----------------------------------
    "IsRootEvent",             # 26 – 1 if sentence is in lead paragraph, else 0
    "EventCode",               # 27 – 4-digit CAMEO action code (e.g., 1730)
    "EventBaseCode",           # 28 – level-2 parent of the action code
    "EventRootCode",           # 29 – level-1 root of the action code
    "QuadClass",               # 30 – 1=VerbalCoop 2=MatCoop 3=VerbConf 4=MatConf
    "GoldsteinScale",          # 31 – impact weight (–10 … +10) assigned to action
    "NumMentions",             # 32 – mentions in the 15-min ingest slice
    "NumSources",              # 33 – distinct sources in that slice
    "NumArticles",             # 34 – distinct documents in that slice
    "AvgTone",                 # 35 – mean doc-level tone (–100 … +100)

    # -------- geography : actor 1 --------------------------------------
    "Actor1Geo_Type",          # 36 – 1=country, 2=US-state, 3=US-city, 4=world-city, 5=world-state
    "Actor1Geo_Fullname",      # 37 – “City/Region, ADM1, Country” human label
    "Actor1Geo_CountryCode",   # 38 – 2-letter FIPS10-4 country code
    "Actor1Geo_ADM1Code",      # 39 – Country+ADM1 FIPS (e.g., ‘USNY’)
    "Actor1Geo_ADM2Code",      # 40 – GAUL ADM2 or US county code
    "Actor1Geo_Lat",           # 41 – centroid latitude
    "Actor1Geo_Long",          # 42 – centroid longitude
    "Actor1Geo_FeatureID",     # 43 – GNS/GNIS feature ID

    # -------- geography : actor 2 --------------------------------------
    "Actor2Geo_Type",          # 44 – location resolution for actor 2
    "Actor2Geo_Fullname",      # 45 – human-readable place name
    "Actor2Geo_CountryCode",   # 46 – country code
    "Actor2Geo_ADM1Code",      # 47 – ADM1 code
    "Actor2Geo_ADM2Code",      # 48 – ADM2 code
    "Actor2Geo_Lat",           # 49 – latitude
    "Actor2Geo_Long",          # 50 – longitude
    "Actor2Geo_FeatureID",     # 51 – feature ID

    # -------- geography : action location ------------------------------
    "ActionGeo_Type",          # 52 – resolution of the action location
    "ActionGeo_Fullname",      # 53 – place tied to the verb phrase
    "ActionGeo_CountryCode",   # 54 – country code
    "ActionGeo_ADM1Code",      # 55 – ADM1 code
    "ActionGeo_ADM2Code",      # 56 – ADM2 code
    "ActionGeo_Lat",           # 57 – latitude
    "ActionGeo_Long",          # 58 – longitude
    "ActionGeo_FeatureID",     # 59 – feature ID

    # -------- bookkeeping ----------------------------------------------
    "DATEADDED",               # 60 – UTC timestamp (YYYYMMDDhhmmss) when row entered GDELT
    "SOURCEURL"                # 61 – first article or citation that generated the event
]

mentions_cols = [
    "GlobalEventID",            # 01 – unique 64-bit identifier for this event row
    "SQLDATE",                  # 02 – event’s SQLDATE as Unix ms  (YYYYMMDDhhmmss * 1000)
    "MentionTimeDate",          # 03 – when THIS mention was published / first seen (Unix ms)
    "MentionType",              # 04 – 1=story lead, 2=story text, 3=blog, etc.
    "MentionSourceName",        # 05 – outlet ID (domain-style string)
    "MentionIdentifier",        # 06 – the article URL (or broadcast clip ID)
    "SentenceID",               # 07 – which sentence inside the article (1-based)
    "Actor1CharOffset",         # 08 – character offset where Actor1 starts in that sentence
    "Actor2CharOffset",         # 09 – …Actor2 starts
    "ActionCharOffset",         # 10 – …the action verb starts
    "InRawText",                # 11 – 1 if the verb phrase appears verbatim, 0 if inferred
    "Confidence",               # 12 – system confidence (0-100)
    "MentionDocLen",            # 13 – entire document length in words
    "MentionDocTone",           # 14 – tone score of the whole document (–100 … +100)
    "MentionDocTranslationInfo",# 15 – (usually blank) info on machine-translated text
    "Extras"                    # 16 – JSON bundle for future fields (blank pre-2023)
]

details_cols = [
    "GKGRECORDID",                 # 01 – unique ID = yyyymmddhhmmss-sequence
    "DATE",                        # 02 – ingest timestamp (UTC, YYYYMMDDhhmmss)
    "SourceCollectionIdentifier",  # 03 – 1=Web, 2=Broadcast/Print, 15=Twitter, etc.
    "SourceCommonName",            # 04 – outlet/domain name (“nytimes.com”)
    "DocumentIdentifier",          # 05 – URL or broadcast citation

    # —–– COUNT & THEME VECTORS ––––––––––––––––––––––––––––––––––––––––––
    "Counts",                      # 06 – legacy CAMEO “theme,count” pairs
    "V2Counts",                    # 07 – enhanced counts (with char offsets)
    "Themes",                      # 08 – legacy pipe-delimited themes
    "V2Themes",                    # 09 – enhanced themes (semicolon list)

    # —–– LOCATION & ENTITY LISTS ––––––––––––––––––––––––––––––––––––––––
    "Locations",                   # 10 – legacy location tuples
    "V2Locations",                 # 11 – enhanced locations (with offsets & conf)
    "Persons",                     # 12 – legacy person list
    "V2Persons",                   # 13 – enhanced persons (name,offset)
    "Organizations",               # 14 – legacy org list
    "V2Organizations",             # 15 – enhanced orgs (name,offset)

    # —–– TEXT-LEVEL SENTIMENT & DATES –––––––––––––––––––––––––––––––––––
    "V2Tone",                      # 16 – “wc:###,c1.1:#,c1.2:#, …”  (word-count + GCAM vector)
    "Dates",                       # 17 – legacy date expressions
    "GCAM",                        # 18 – “wc:###,c1.1:#,c1.2:#, …”  (word-count + GCAM vector)

    # —–– MEDIA & EMBEDS ––––––––––––––––––––––––––––––––––––––––––––––––
    "SharingImage",                # 19 – canonical OpenGraph/Twitter card image URL (if any)
    "RelatedImages",               # 20 – other images scraped from the page (comma list)
    "SocialImageEmbeds",           # 21 – embedded social-platform images
    "SocialVideoEmbeds",           # 22 – embedded social videos (YouTube, Vimeo, …)

    # —–– QUOTATIONS –––––––––––––––––––––––––––––––––––––––––––––––––––––
    "QuotedPersons",               # 23 – raw quoted-speaker names (“John Smith; …”)
    "QuotedPersonsCanonical",      # 24 – canonical person IDs if resolvable
    "Quotations",                  # 25 – the quotation strings themselves

    # —–– NER / MONEY / TRANS ––––––––––––––––––––––––––––––––––––––––––––
    "AllNames",                    # 26 – every proper name string (not just persons/orgs)
    "Amounts",                     # 27 – money and quantity expressions (“USD 5 million”)
    "TranslationInfo",             # 28 – details if the page was machine-translated
    "Extras"                       # 29 – JSON bundle reserved for future fields
]

pattern = re.compile(r"(?:^|,)wc:(\d+)|c3\.1:(\d+)|c3\.2:(\d+)|c4\.16:(\d+)")

def extract_gcam(gcam: str):
    wc = negative = positive = finance = 0
    for m in pattern.finditer(gcam):
        if m.group(1):   # wc      WORD COUNT
            wc = int(m.group(1))
        elif m.group(2): # c3.1    NEGATIVE
            negative = int(m.group(2))
        elif m.group(3): # c3.2    POSITIVE
            positive = int(m.group(3))
        elif m.group(4): # c4.16   FINANCE
            finance = int(m.group(4))
    return pd.Series([wc, negative, positive, finance])

def make_file_path(data_type: str, year: str, date: str):
    return os.path.join("data/files/", data_type, year, f"{date}.csv.gz")


async def fetch_bytes(session: aiohttp.ClientSession, url: str) -> bytes:
    async with session.get(url) as r:
        r.raise_for_status()
        return await r.read()


def _maybe_decompress(raw: bytes) -> bytes:
    if raw[:2] == b"\x1f\x8b":
        return gzip.decompress(raw)

    if raw[:4] == b"PK\x03\x04":
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            name = zf.namelist()[0]
            return zf.read(name)

    return raw


def parse_csv(raw: bytes, data_type: str, path: str) -> None:
    names = {"event": event_cols, "mention": mentions_cols, "detail": details_cols}[data_type]
    columns = {
        "event": ["GlobalEventID", "SQLDATE", "EventBaseCode", "QuadClass", "GoldsteinScale", "ActionGeo_CountryCode"],
        "mention": ["GlobalEventID", "MentionTimeDate", "MentionIdentifier"],
        "detail": ["DocumentIdentifier", "WordCount", "Negative", "Positive", "Finance"]
    }[data_type]

    plain = _maybe_decompress(raw)

    df: pd.DataFrame = pd.DataFrame()
    for enc in ("utf-8", "latin-1", None):
        try:
            df = pd.read_csv(
                io.BytesIO(plain),
                sep="\t",
                header=None,
                names=names,
                encoding=enc if enc is not None else "utf-8",
                encoding_errors="replace" if enc is not None else "strict",
                compression="infer",
            )
            break
        except UnicodeDecodeError:
            pass


    if data_type == "detail":
        df = df[df['GCAM'].notna()]
        df[["WordCount", "Negative", "Positive", "Finance"]] = df["GCAM"].apply(extract_gcam)

    df = df[columns]

    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, compression="gzip")


async def download_dataframe(url: str, data_type: str, year: str, date: str, pool: ProcessPoolExecutor, cache: bool = False) -> None:
    file_path = make_file_path(data_type, year, date)
    if cache and os.path.exists(file_path):
        return

    async with aiohttp.ClientSession() as session:
        raw = await fetch_bytes(session, url)
    loop = asyncio.get_running_loop()

    await loop.run_in_executor(pool, parse_csv, raw, data_type, file_path)

def _init_worker():
    faulthandler.enable()
    sys.excepthook = lambda exc, val, tb: print("WORKER EXCEPTION:","".join(traceback.format_exception(exc, val, tb)),file=sys.stderr, flush=True)


async def download_all(master: pd.DataFrame, data_type: str, cache: bool = False, concurrency: int = 10) -> None:
    old = gc.isenabled()
    gc.disable()
    total = len(master)
    semaphore = asyncio.Semaphore(concurrency)

    progress = make_progress()
    task_id = progress.add_task(f"Downloading", total=total)


    with progress:
        with ProcessPoolExecutor(max_workers=concurrency, initializer=_init_worker) as pool:
            async with asyncio.TaskGroup() as task_group:
                for url, year, date in master[["url", "year", "date"]].itertuples(False):
                    async def _worker(u=url, y=str(year), d=date):
                        async with semaphore:
                            try:
                                await download_dataframe(u, data_type, y, d, pool, cache)
                            except ClientResponseError as exc:
                                progress.console.print(f"[red] {d} failed: {type(exc).__name__}[/]  {exc}")
                            except ParserError as exc:
                                progress.console.print(f"[red] {d} failed: {type(exc).__name__}[/]  {exc}")
                                if str(exc) != "Empty CSV file":
                                    pass
                            except Exception as exc:
                                progress.console.print(f"[red] {d} failed: {type(exc).__name__}[/]  {exc}")
                            finally:
                                progress.update(task_id, advance=1)
                    task_group.create_task(_worker())
    if old:
        gc.enable()