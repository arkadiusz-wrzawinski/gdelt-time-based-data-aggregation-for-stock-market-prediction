import asyncio
import gc
import os
from concurrent.futures.process import ProcessPoolExecutor
import pandas as pd

from downloading import _init_worker, make_file_path
from progress import make_progress

def make_quarter_path(data_type: str, year: int, quarter: int):
    return os.path.join("data/files/quarters/", data_type, f"{str(year)}-{str(quarter)}.csv.gz")

def join_files(files: list[str], dirname: str, data_type: str, year: int, quarter: int) -> None:
    df = pd.concat(map(pd.read_csv, [os.path.join(dirname, file) for file in files]))

    if data_type == "event":
        df = df[df["EventBaseCode"] != ""]
        df = df[df["EventBaseCode"] != "---"]
        df["EventBaseCode"] = df["EventBaseCode"].astype(str).str.zfill(3).str[0:2].astype(int)

    path = make_quarter_path(data_type, year, quarter)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, compression="gzip")


async def quarterize(quarters_in_years: list[tuple[list[int], int]], data_type: str, cache: bool = False) -> None:
    old = gc.isenabled()
    gc.disable()
    semaphore = asyncio.Semaphore(10)

    total = len(quarters_in_years) * 4

    progress = make_progress()
    task_id = progress.add_task(f"Quarterizing", total=total)

    with progress:
        with ProcessPoolExecutor(max_workers=10, initializer=_init_worker) as pool:
            async with asyncio.TaskGroup() as task_group:
                for quarters, year in quarters_in_years:
                    dirname = os.path.dirname(make_file_path(data_type, str(year), ""))
                    files = os.listdir(dirname)
                    quarterized_files = [[], [], [], []]
                    for file in files:
                        month = int(file[4:6])
                        if month < 4:
                            quarterized_files[0].append(file)
                        elif month < 7:
                            quarterized_files[1].append(file)
                        elif month < 10:
                            quarterized_files[2].append(file)
                        else:
                            quarterized_files[3].append(file)

                    async def _worker(f: list[str], d: str, dt: str, y: int, q: int):
                        async with semaphore:
                            try:
                                loop = asyncio.get_running_loop()
                                await loop.run_in_executor(pool, join_files, f, d, dt, y, q)
                            finally:
                                progress.update(task_id, advance=1)

                    for index, file_list in enumerate(quarterized_files):
                        task_group.create_task(_worker(file_list, dirname, data_type, year, index + 1))

    if old:
        gc.enable()
