import asyncio
import gc
import os
from concurrent.futures.process import ProcessPoolExecutor
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import joblib

from downloading import _init_worker
from progress import make_progress

def make_financial_path(name: str, is_raw: bool):
    return os.path.join("data/files/financial/", "raw" if is_raw else "processed", f"{name}.csv")

def make_scaler_path(name: str):
    return os.path.join("data/files/financial/processed/scalers", f"{name}.joblib.gz")

def process_financial_file(name: str):
    df = pd.read_csv(make_financial_path(name, is_raw=True))

    df["Close"] = df["Price"].str.replace(",", "").astype(float)
    df["CloseToClose"] = df["Close"].pct_change(periods=-1) * 100
    scaler = MinMaxScaler(feature_range=(0, 1))
    df["CloseToClose"] = scaler.fit_transform(df[["CloseToClose"]])

    df["Date"] = pd.to_datetime(df['Date'], format="%m/%d/%Y")

    df = df[["Date", "CloseToClose"]]

    os.makedirs(os.path.dirname(make_financial_path(name, is_raw=False)), exist_ok=True)
    df.to_csv(make_financial_path(name, is_raw=False), index=False)

    scaler_path = make_scaler_path(name)
    os.makedirs(os.path.dirname(scaler_path), exist_ok=True)
    joblib.dump(scaler, scaler_path)

async def process_all_financial_files(names: list[str]):
    old = gc.isenabled()
    gc.disable()
    semaphore = asyncio.Semaphore(16)

    total = len(names)

    progress = make_progress()
    task_id = progress.add_task(f"Processing financial data", total=total)

    with progress:
        with ProcessPoolExecutor(max_workers=16, initializer=_init_worker) as pool:
            async with asyncio.TaskGroup() as task_group:
                async def _worker(n: str):
                    async with semaphore:
                        try:
                            loop = asyncio.get_running_loop()
                            await loop.run_in_executor(pool, process_financial_file, n)
                        finally:
                            progress.update(task_id, advance=1)
                for name in names:
                    task_group.create_task(_worker(name))

    if old:
        gc.enable()