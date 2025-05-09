import asyncio
import gc
import os
from concurrent.futures.process import ProcessPoolExecutor
import pandas as pd

from downloading import _init_worker
from progress import make_progress

def make_quarter_path(data_type: str, year: int, quarter: int):
    return os.path.join("data/files/quarters/", data_type, f"{str(year)}-{str(quarter)}.csv.gz")

def make_collected_path(year: int, quarter: int):
    return os.path.join("data/files/collected/", f"{str(year)}-{str(quarter)}.csv.gz")

def make_next_quarter(year: int, quarter: int) -> (int, int):
    if quarter == 4:
        return year + 1, 1
    return year, quarter + 1


def collect_data(year: int, quarter: int):
    next_year, next_quarter = make_next_quarter(year, quarter)

    events_path = os.path.abspath(make_quarter_path("event", year, quarter))

    mentions_path_1 = os.path.abspath(make_quarter_path("mention", year, quarter))
    mentions_path_2 = os.path.abspath(make_quarter_path("mention", next_year, next_quarter))

    details_path_1 = os.path.abspath(make_quarter_path("detail", year, quarter))
    details_path_2 = os.path.abspath(make_quarter_path("detail", next_year, next_quarter))

    if os.path.exists(events_path) and os.path.exists(mentions_path_1) and os.path.exists(details_path_1):

        events = pd.read_csv(events_path)

        mentions = pd.read_csv(mentions_path_1)
        if os.path.exists(mentions_path_2):
            mentions = pd.concat([mentions, pd.read_csv(mentions_path_2)])

        details = pd.read_csv(details_path_1)
        if os.path.exists(details_path_2):
            details = pd.concat([details, pd.read_csv(details_path_2)])

        events['time'] = pd.to_datetime(events['SQLDATE'], format='%Y%m%d')
        mentions['time'] = pd.to_datetime(mentions['MentionTimeDate'], format='%Y%m%d%H%M%S')

        em = pd.merge(
            events,
            mentions,
            on='GlobalEventID',
            how='left',
            suffixes=('_e', '_m')
        )

        em = em[(em['time_m'] >= em['time_e']) & (em['time_m'] <= em['time_e'] + pd.Timedelta(weeks=1))]

        em = pd.merge(
            em,
            details,
            left_on="MentionIdentifier",
            right_on="DocumentIdentifier",
            how='left'
        )

        result = (
            em
            .groupby(
                ['GlobalEventID', 'time_e', 'EventBaseCode', 'QuadClass', 'GoldsteinScale', 'ActionGeo_CountryCode'],
                as_index=False
            )
            .agg(
                MentionsCount=('MentionIdentifier', 'size'),
                WordCount=('WordCount', 'sum'),
                Negative=('Negative', 'sum'),
                Positive=('Positive', 'sum'),
                Finance=('Finance', 'sum')
            )
            .rename(columns={'time_e': 'Time'})
        )

        collected_path = make_collected_path(year, quarter)
        os.makedirs(os.path.dirname(collected_path), exist_ok=True)
        result.to_csv(collected_path, index=False, compression="gzip")


async def collect_all(quarters_in_years: list[tuple[list[int], int]], cache: bool = False) -> None:
    old = gc.isenabled()
    gc.disable()
    semaphore = asyncio.Semaphore(2)

    total = len(quarters_in_years) * 4

    progress = make_progress()
    task_id = progress.add_task(f"Collecting", total=total)

    with progress:
        with ProcessPoolExecutor(max_workers=2, initializer=_init_worker) as pool:
            async with asyncio.TaskGroup() as task_group:
                for quarters, year in quarters_in_years:
                    async def _worker(y: int, q: int):
                        async with semaphore:
                            try:
                                loop = asyncio.get_running_loop()
                                await loop.run_in_executor(pool, collect_data, y, q)
                            finally:
                                progress.update(task_id, advance=1)
                    for quarter in quarters:
                        task_group.create_task(_worker(year, quarter))
    if old:
        gc.enable()